package streamutil

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"

	"github.com/DanielSvub/stream"
)

type FileMode uint8

const (
	FileWrite = iota
	FileAppend
)

type NdjsonInput[T any] interface {
	stream.Producer[T]
}

type NdjsonOutput[T any] interface {
	stream.Consumer[T]
	Run(func(T)) error
}

type ndjsonInput[T any] struct {
	stream.DefaultClosable
	stream.DefaultProducer[T]
	path    string
	file    *os.File
	scanner *bufio.Scanner
}

func NewNdjsonInput[T any](path string) NdjsonInput[T] {
	ego := &ndjsonInput[T]{path: path}
	ego.DefaultProducer = *stream.NewDefaultProducer[T](ego)
	return ego
}

func (ego *ndjsonInput[T]) Get() (value T, valid bool, err error) {

	if ego.file == nil {
		var file *os.File
		file, err = os.Open(ego.path)
		if err != nil {
			return
		}
		ego.file = file
		ego.scanner = bufio.NewScanner(file)
	}

	valid = ego.scanner.Scan()

	if valid {
		err = json.Unmarshal([]byte(ego.scanner.Text()), &value)
		if err != nil {
			return
		}
	} else {
		ego.file.Close()
		ego.Close()
	}

	return

}

type ndjsonOutput[T any] struct {
	stream.DefaultConsumer[T]
	path string
	mode FileMode
	file *os.File
}

func NewNdjsonOutput[T any](path string, mode FileMode) NdjsonOutput[T] {

	if mode != FileAppend && mode != FileWrite {
		panic("unknown mode")
	}

	ego := &ndjsonOutput[T]{}

	ego.mode = mode
	ego.path = path

	return ego

}

func (ego *ndjsonOutput[T]) Run(callback func(T)) error {

	if ego.file != nil {
		return errors.New("the stream has been already run")
	}

	var flags int
	if ego.mode == FileWrite {
		flags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	} else {
		flags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	}

	file, err := os.OpenFile(ego.path, flags, 0664)
	if err != nil {
		return err
	}
	ego.file = file

	for {
		value, valid, err := ego.Consume()
		if !valid || err != nil {
			break
		}
		nd, err := json.Marshal(value)
		if err != nil {
			break
		}
		_, err = ego.file.Write(nd)
		if err != nil {
			break
		}
		_, err = ego.file.WriteString("\n")
		if err != nil {
			break
		}
		if callback != nil {
			callback(value)
		}
	}

	ego.file.Close()

	return err

}
