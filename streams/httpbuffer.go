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
)

//TODO logging

func max(a, b int) int { //TODO really, go?
	if a < b {
		return b
	}
	return a
}

type HttpInputStreamer[T any] interface {
	InputStreamer[T]
}

type HttpOutputStreamer[T any] interface {
	OutputStreamer[T]
	Handle(w http.ResponseWriter, r *http.Request)
}

// transferData wraps stream output values, so we can serialize them into json easily
type transferData[T any] struct {
	Value T
	Valid bool
	Error error
}

type httpOutputStream[T any] struct {
	outputStream[T]
}

func NewHttpOutputStream[T any]() HttpOutputStreamer[T] {
	ego := &httpOutputStream[T]{}
	ego.init(ego)
	return ego
}

func (ego *httpOutputStream[T]) Handle(w http.ResponseWriter, r *http.Request) {
	var itemCount int
	var err error
	ic := r.URL.Query().Get("itemCount")
	if itemCount, err = strconv.Atoi(ic); err != nil {
		itemCount = 1
	}
	bw := bufio.NewWriter(w)
	for i := 0; i < itemCount; i++ {
		value, valid, err := ego.source.get() //we are blocking here if there is not enough data
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

type httpInputStream[T any] struct {
	inputStream[T]
	server string
	port   int16
}

func NewHttpInputStream[T any](server string, port int16) HttpInputStreamer[T] {
	ego := &httpInputStream[T]{
		server: server,
		port:   port,
	}
	ego.init(ego)
	return ego
}

func (ego *httpInputStream[T]) get() (T, bool, error) {
	resp, err := http.Get(fmt.Sprintf("%s:%d?itemCount=1", ego.server, ego.port)) //get returns single item -> we ask for single item
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

type bufferedHttpInputStream[T any] struct {
	inputStream[T]
	server     string
	port       int16
	buffer     chan transferData[T]
	bufferSize int
}

func NewBufferedHttpInputStream[T any](server string, port int16, bufferSize int) HttpInputStreamer[T] {
	ego := &bufferedHttpInputStream[T]{
		server:     server,
		port:       port,
		buffer:     make(chan transferData[T], bufferSize),
		bufferSize: bufferSize,
	}
	ego.init(ego)
	return ego
}

func (ego *bufferedHttpInputStream[T]) get() (T, bool, error) {
	select {
	case transfer := <-ego.buffer:
		return transfer.Value, transfer.Valid, transfer.Error
	default:
		ego.fillBuffer()
		return ego.get()
	}

}

func (ego *bufferedHttpInputStream[T]) fillBuffer() (int, error) {
	resp, err := http.Get(fmt.Sprintf("%s:%d?itemCount=%d", ego.server, ego.port, ego.bufferSize))
	if err != nil {
		log.Fatal("Could not reach the server: ", err)
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, errors.New(fmt.Sprintf("Server responded with ", resp.StatusCode))
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

func (ego *bufferedHttpInputStream[T]) Close() {
	close(ego.buffer)
	ego.close()
}

func (ego *bufferedHttpInputStream[T]) Closed() bool {
	return ego.closed && len(ego.buffer) == 0
}
