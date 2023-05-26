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
var hos HttpOutputStream[*TestObject]
var outs ReadableOutputStreamer[*TestObject]
var server *http.Server
var port int16

var testData []*TestObject
var testDataCount int

func createSource() {
	is = NewBufferInputStream[*TestObject](testDataCount)
	is.Pipe(hos)
}

func fillData() {
	testData = make([]*TestObject, testDataCount)
	for i := 0; i < testDataCount; i++ {
		testData[i] = NewTestObject(fmt.Sprintf("%d", i))
		is.Write(testData[i])
	}
	is.Close()
}

func setupServer() {
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
	time.Sleep(1 * time.Second) //TODO this seems to be antipattern, what is a correct way?

}

func stopServer() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Default().Println(err)
	}
}

func TestStreamHttpConverter(t *testing.T) {
	testDataCount = 100
	hos = NewStreamToHttpConverter[*TestObject]()
	setupServer()
	createSource()
	go fillData()
	t.Run("Pull 'streaming', pipeline conversion to http", func(t *testing.T) {
		his := NewHttpToStreamConverter[*TestObject]("http://127.0.0.1", port, "")
		outs = NewReadableOutputStream[*TestObject]()
		his.Pipe(outs)
		var result []*TestObject
		var err error
		if result, err = outs.Collect(); err != nil {
			fmt.Println(err)
		}
		if len(result) != testDataCount {
			t.Error("Diffrent data count sent and red", testDataCount, len(result))
		}
		for i, r := range result {
			if testData[i].Text != r.Text {
				t.Error(fmt.Sprintf("Different data sent and red. %v, %v", testData[i], r))
			}
		}

	})
	stopServer()
}

func TestBufferedStreamHttpConverter(t *testing.T) {
	testDataCount = 100
	hos = NewStreamToHttpConverter[*TestObject]()
	setupServer()
	createSource()
	go fillData()
	t.Run("Pull 'streaming' with buffering, pipeline conversion to http", func(t *testing.T) {
		his := NewBufferedHttpToStreamConverter[*TestObject]("http://127.0.0.1", port, "", 2)
		outs = NewReadableOutputStream[*TestObject]()
		his.Pipe(outs)
		result := make([]*TestObject, 1)
		for i := 0; i < testDataCount; i++ {
			if _, err := outs.Read(result); err != nil {
				t.Error(err)
				continue
			}
			if testData[i].Text != result[0].Text {
				t.Error(fmt.Sprintf("Different data sent and red. %v, %v", testData[i], result[0]))
			}
		}

	})
	stopServer()
}

func TestHttpStreaming(t *testing.T) {
	testDataCount = 500
	hos = NewHttpOutputStream[*TestObject]()
	setupServer()
	createSource()
	go fillData()
	t.Run("Proper http streaming through response", func(t *testing.T) {
		his := NewHttpInputStream[*TestObject]("http://127.0.0.1", port, "", 10)
		outs = NewReadableOutputStream[*TestObject]()
		his.Pipe(outs)
		result := make([]*TestObject, 1)
		for i := 0; i < testDataCount; i++ {
			if n, err := outs.Read(result); err != nil || n == 0 {
				t.Error(err)
				continue
			}
			if testData[i].Text != result[0].Text {
				t.Error(fmt.Sprintf("Different data sent and red. %v, %v", testData[i], result[0]))
			}

		}

	})
	stopServer()
}
