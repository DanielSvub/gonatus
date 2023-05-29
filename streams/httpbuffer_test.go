package streams_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	. "github.com/SpongeData-cz/gonatus/streams"
)

// TestObject is simple object for testing
type TestObject struct {
	Text  string
	Value int
}

func NewTestObject(text string, value int) *TestObject {
	res := &TestObject{Text: text, Value: value}
	return res
}

func (ego TestObject) String() string {
	return fmt.Sprintf("Text: %s, Value: %d", ego.Text, ego.Value)
}

func (ego TestObject) equals(other TestObject) bool {
	return ego.Text == other.Text && ego.Value == other.Value
}

var is BufferInputStreamer[*TestObject]
var hos HttpOutputStream[*TestObject]
var outs ReadableOutputStreamer[*TestObject]

var srv *httptest.Server
var serverAddress string
var port uint16

var testData []*TestObject
var testDataCount int

func createSource() {
	is = NewBufferInputStream[*TestObject](testDataCount)
	is.Pipe(hos)
}

func fillData() {
	testData = make([]*TestObject, testDataCount)
	for i := 0; i < testDataCount; i++ {
		testData[i] = NewTestObject(fmt.Sprintf("%d", i), i)
		is.Write(testData[i])
	}
	is.Close()
}

func setupServer() {
	port = uint16(9991)
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hos.Handle(w, r)
	}))
	parts := strings.Split(srv.URL, ":")
	serverAddress = parts[0] + ":" + parts[1]
	prtInt, _ := strconv.Atoi(parts[2])
	port = uint16(prtInt)

}

func stopServer() {
	srv.Close()
}

func initTest(dataCount int) {
	testDataCount = dataCount
	setupServer()
	createSource()
	go fillData()
}

func TestStreamHttpConverter(t *testing.T) {
	hos = NewStreamToHttpConverter[*TestObject]()
	initTest(1000)
	t.Run("Pull 'streaming', pipeline conversion to http", func(t *testing.T) {
		his := NewHttpToStreamConverter[*TestObject](serverAddress, port, "")
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
			if !testData[i].equals(*r) {
				t.Error(fmt.Sprintf("Different data sent and red. %v, %v", testData[i], r))
			}
		}

	})
	stopServer()
}

func TestBufferedStreamHttpConverter(t *testing.T) {
	hos = NewStreamToHttpConverter[*TestObject]()
	initTest(1000)
	t.Run("Pull 'streaming' with buffering, pipeline conversion to http", func(t *testing.T) {
		his := NewBufferedHttpToStreamConverter[*TestObject](serverAddress, port, "", 2)
		outs = NewReadableOutputStream[*TestObject]()
		his.Pipe(outs)
		result := make([]*TestObject, 1)
		for i := 0; i < testDataCount; i++ {
			if _, err := outs.Read(result); err != nil {
				t.Error(err)
				continue
			}
			if !testData[i].equals(*result[0]) {
				t.Error(fmt.Sprintf("Different data sent and red. %v, %v", testData[i], result[0]))
			}
		}

	})
	stopServer()
}

func TestHttpStreaming(t *testing.T) {
	hos = NewHttpOutputStream[*TestObject]()
	initTest(1000)
	t.Run("Proper http streaming through response", func(t *testing.T) {
		his := NewHttpInputStream[*TestObject](serverAddress, port, "", 10)
		outs = NewReadableOutputStream[*TestObject]()
		his.Pipe(outs)
		result := make([]*TestObject, 1)
		for i := 0; i < testDataCount; i++ {
			if n, err := outs.Read(result); err != nil || n == 0 {
				t.Error(err)
				continue
			}
			if !testData[i].equals(*result[0]) {
				t.Error(fmt.Sprintf("Different data sent and red. %v, %v", testData[i], result[0]))
			}

		}

	})
	stopServer()
}
