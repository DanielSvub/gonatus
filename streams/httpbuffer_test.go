package streams_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	. "github.com/SpongeData-cz/gonatus/streams"
)

type TestObject struct {
	Text string
}

func NewTestObject(text string) *TestObject {
	res := &TestObject{Text: text}
	return res
}

func (ego TestObject) String() string {
	return fmt.Sprintf("Text: %s", ego.Text)
}

var is BufferInputStreamer[*TestObject]
var hos HttpOutputStreamer[*TestObject]
var outs ReadableOutputStreamer[*TestObject]
var server *http.Server
var port int16

var testData []*TestObject
var testDataCount int

func createSource() {
	fmt.Println("Creating source stream")
	testDataCount = 10
	is = NewBufferInputStream[*TestObject](testDataCount)
	testData = make([]*TestObject, testDataCount)
	for i := 0; i < testDataCount; i++ {
		testData[i] = NewTestObject(fmt.Sprintf("%d", i))
		is.Write(testData[i])
		fmt.Printf("Data sent: %+v\n", testData[i])
	}
	is.Close()
	is.Pipe(hos)
	fmt.Println("Source stream created and piped")
}

func setupServer() {

	fmt.Println("Setup server side")
	hos = NewHttpOutputStream[*TestObject]()
	fmt.Println("Trying to run server")
	port = int16(9991)
	server = &http.Server{
		Addr: fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hos.Handle(w, r)
		})}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Default().Println(err)
		}
	}()
	time.Sleep(1 * time.Second)
	fmt.Println("Server is listening on port", port)
	fmt.Println("Server setup done")
}

func stopServer() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Default().Println(err)
	}
}

func TestHttpStream(t *testing.T) {
	setupServer()
	createSource()
	t.Run("Send and recieve data", func(t *testing.T) {

		his := NewHttpInputStream[*TestObject]("http://127.0.0.1", port)
		outs = NewReadableOutputStream[*TestObject]()
		fmt.Println("Client HttpInputStream created, piping ReadableOutpuStream")
		his.Pipe(outs)

		var result []*TestObject
		var err error

		if result, err = outs.Collect(); err != nil {
			fmt.Println(err)
		}
		i := 0
		for _, r := range result {
			fmt.Println(fmt.Sprintf("Data red:  %+v", r))
			if testData[i].Text != r.Text {
				t.Error(fmt.Sprintf("Different data sent and red. %v, %v", testData[i], r))
			}
			i++
		}

	})
	stopServer()
}

func TestBufferedHttpStream(t *testing.T) {
	setupServer()
	createSource()
	t.Run("Send and recieve data", func(t *testing.T) {

		his := NewBufferedHttpInputStream[*TestObject]("http://127.0.0.1", port, 2)
		outs = NewReadableOutputStream[*TestObject]()
		fmt.Println("Client HttpInputStream created, piping ReadableOutpuStream")
		his.Pipe(outs)
		result := make([]*TestObject, 1)

		for i := 0; i < testDataCount; i++ {
			var n int
			var err error
			if n, err = outs.Read(result); err != nil {
				fmt.Println(err)
			}
			fmt.Println(fmt.Sprintf("%d Data red:  %+v", n, result))
			if testData[i].Text != result[0].Text {
				t.Error(fmt.Sprintf("Different data sent and red. %v, %v", testData[i], result[0]))
			}
		}

	})
	stopServer()
}
