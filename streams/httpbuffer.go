package streams

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
)

//TODO proper logging (request tracing etc.)

func max(a, b int) int { //TODO really, go?
	if a < b {
		return b
	}
	return a
}

type HttpInputStream[T any] interface {
	InputStreamer[T]
}

type HttpOutputStream[T any] interface {
	OutputStreamer[T]
	Handle(w http.ResponseWriter, r *http.Request)
}

// transferData wraps stream output values, so we can serialize them into json easily
type transferData[T any] struct {
	Value T
	Valid bool
	Error error
}

/*
Stream pipeline to http converter: producer side of pull based data "stream" over http - converts stream pipeline to http.

Extends:
  - outputStream.

Implements:
  - HttpOutputStreamer
*/
type streamToHttpConverter[T any] struct {
	outputStream[T]
}

/*
Stream to http converter constructor

Parameters:
*/
func NewStreamToHttpConverter[T any]() HttpOutputStream[T] {
	ego := &streamToHttpConverter[T]{}
	ego.init(ego)
	return ego
}

// Handle is handler function which should be registered by server router for an endpoint
func (ego *streamToHttpConverter[T]) Handle(w http.ResponseWriter, r *http.Request) {
	var itemCount int
	var err error
	ic := r.URL.Query().Get("itemCount")
	if itemCount, err = strconv.Atoi(ic); err != nil {
		itemCount = 1
	}
	bw := bufio.NewWriter(w)
	for i := 0; i < itemCount; i++ {
		value, valid, err := ego.source.Get() //we are blocking here if there is not enough data (implementation details of get)
		data := transferData[T]{Value: value, Valid: valid, Error: err}
		resp, err := json.Marshal(data)
		bw.Write(resp)
		if i != itemCount {
			bw.WriteRune('\n')
		}
	}

	w.WriteHeader(http.StatusOK)
	bw.Flush()
}

/*
Http to stream converter: consumer side of pull based data "stream" over http - converting to stream pipeline.

Extends:
  - inputStream.

Implements:
  - HttpInputStreamer
*/
type httpToStreamConverter[T any] struct {
	inputStream[T]
	server string
	port   uint16
	path   string
}

//TODO keep it atomic or use just URL?

/*
Http to stream converter constructor.

Parameters:
  - server - server address
  - port - server port
  - path - endpoint
  - bufferSize - size of buffer

Returns:
  - pointer to the created http input stream
*/
func NewHttpToStreamConverter[T any](server string, port uint16, path string) HttpInputStream[T] {
	ego := &httpToStreamConverter[T]{
		server: server,
		port:   port,
		path:   path,
	}
	ego.init(ego)
	return ego
}

func (ego *httpToStreamConverter[T]) Get() (T, bool, error) {
	resp, err := http.Get(fmt.Sprintf("%s:%d/%s?itemCount=1", ego.server, ego.port, ego.path)) //stream get returns single item -> we ask for single item
	if err != nil {
		log.Fatal("Could not reach the server: ", err)
		return *new(T), false, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return *new(T), false, errors.New(string(body))
	}
	transfer := &transferData[T]{}

	if err = json.Unmarshal(body, transfer); err != nil {
		log.Default().Println("Error (", err, ") while deserializing received data: ", string(body)) // TODO better loging....
		return *new(T), false, err
	}
	return transfer.Value, transfer.Valid, transfer.Error

}

/*
Buffered http to stream converter: consumer side of pull based data "stream" over http - converting to stream pipeline.

Extends:
  - inputStream.

Implements:
  - HttpInputStreamer
*/
type bufferedHttpToStreamConverter[T any] struct {
	inputStream[T]
	server     string
	port       uint16
	path       string
	buffer     chan transferData[T]
	bufferSize int
}

//TODO beware of Java naming style

/*
Buffered http to stream converter constructor.

Parameters:
  - server - server address
  - port - server port
  - path - endpoint
  - bufferSize - size of buffer

Returns:
  - pointer to the created http input stream
*/
func NewBufferedHttpToStreamConverter[T any](server string, port uint16, path string, bufferSize int) HttpInputStream[T] {
	ego := &bufferedHttpToStreamConverter[T]{
		server:     server,
		port:       port,
		path:       path,
		buffer:     make(chan transferData[T], bufferSize),
		bufferSize: bufferSize,
	}
	ego.init(ego)
	return ego
}

func (ego *bufferedHttpToStreamConverter[T]) Get() (T, bool, error) {
	select {
	case transfer := <-ego.buffer:
		return transfer.Value, transfer.Valid, transfer.Error
	default:
		ego.fillBuffer()
		return ego.Get()
	}

}

// fillBuffer tries to fill a buffer by call for enough data, returns count of items actually received
func (ego *bufferedHttpToStreamConverter[T]) fillBuffer() (int, error) {
	resp, err := http.Get(fmt.Sprintf("%s:%d/%s?itemCount=%d", ego.server, ego.port, ego.path, ego.bufferSize))
	if err != nil {
		log.Fatal("Could not reach the server: ", err)
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, errors.New(fmt.Sprintf("Server responded with %d", resp.StatusCode))
	}
	breader := bufio.NewReader(resp.Body)
	for i := 0; i < ego.bufferSize; i++ {
		transfer := &transferData[T]{}
		var js []byte
		if js, err = breader.ReadBytes('\n'); err != nil {
			log.Default().Println("Error (", err, ") while deserializing received data: ", js)
			return max(i-1, 0), err
		}
		if err = json.Unmarshal(js, transfer); err != nil {
			log.Default().Println("Error (", err, ") while deserializing received data: ", js)
			return max(i-1, 0), err
		}
		ego.buffer <- *transfer
	}
	return ego.bufferSize, nil
}

func (ego *bufferedHttpToStreamConverter[T]) Close() {
	close(ego.buffer)
	ego.close()
}

func (ego *bufferedHttpToStreamConverter[T]) Closed() bool {
	return ego.closed && len(ego.buffer) == 0
}

/*
Http input stream: A consumer side of a proper http stream. Buffered. It reads data from http resposne and pass them into stream pipeline whrough get.

Extends:
  - inputStream.

Implements:
  - HttpInputStreamer
*/
type httpInputStream[T any] struct {
	inputStream[T]
	server     string
	port       uint16
	path       string
	buffer     chan transferData[T]
	bufferSize int
	connected  bool
	mu         sync.Mutex
}

/*
Http input stream constructor.

Parameters:
  - server - server address
  - port - server port
  - path - endpoint
  - bufferSize - size of bufferSize

Returns:
  - pointer to the created http input stream.
*/
func NewHttpInputStream[T any](server string, port uint16, path string, bufferSize int) HttpInputStream[T] {
	ego := &httpInputStream[T]{
		server:     server,
		port:       port,
		path:       path,
		bufferSize: bufferSize,
		buffer:     make(chan transferData[T], bufferSize),
		connected:  false,
		mu:         sync.Mutex{},
	}
	ego.init(ego)
	return ego
}

func (ego *httpInputStream[T]) Get() (T, bool, error) {
	ego.mu.Lock()
	if !ego.connected {
		go ego.connectAndRead()
	} else {
		ego.mu.Unlock()
	}
	val := <-ego.buffer
	return val.Value, val.Valid, val.Error

}

// connectAndRead pushes data from stream into the buffer
// buffer should be large enough to read all the data, or someone else should read fast enough from the buffer
// there is no defence mechanism against throttling implemented so far (e.g. swapping to disk)!
func (ego *httpInputStream[T]) connectAndRead() error {
	resp, err := http.Get(fmt.Sprintf("%s:%d/%s", ego.server, ego.port, ego.path))
	if err != nil {
		log.Default().Println("Could not reach the server: ", err)
		ego.mu.Unlock()
		return err
	}
	ego.connected = true
	ego.mu.Unlock()
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	for {
		var t transferData[T]
		err := dec.Decode(&t)
		if err != nil {
			log.Println(err)
			return err
		}
		ego.buffer <- t
		if !t.Valid { //invalid data means stream was closed
			return nil
		}

	}
}

/*
Http output stream: A producer side of a proper http stream. It streams data from its InputStreamer source into http resposne through Handle handler function.

Extends:
  - outputStream.

Implements:
  - HttpOutputStreamer
*/
type httpOutputStream[T any] struct {
	outputStream[T]
}

/*
Http output stream constructor.

Parameters:

Returns:
  - pointer to the created http output stream.
*/
func NewHttpOutputStream[T any]() HttpOutputStream[T] {
	ego := &httpOutputStream[T]{}
	ego.init(ego)
	return ego
}

// Handle is handler function to be registered for some endpoint of a server - actually transfers data form InputStreamer to http response until InputStreamer closes.
func (ego *httpOutputStream[T]) Handle(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	for true {
		value, valid, err := ego.source.Get() //we are blocking here if there is no data (implementation details of get)
		data := transferData[T]{Value: value, Valid: valid, Error: err}
		if err = enc.Encode(data); err != nil {
			log.Println("Encoding problem", err)
			continue
		}
		if !valid { // invalid value means input stream is closed
			log.Default().Println("No more valid data, response sent")
			break
		}
	}

}
