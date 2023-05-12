package streams

import (
	"errors"
)

type BufferInputStreamer[T any] interface {
	InputStreamer[T]
	error(err error)
	Write(p ...T) (n int, err error)
	Close()
}

type ReadableOutputStreamer[T any] interface {
	OutputStreamer[T]
	Read(p []T) (n int, err error)
	Collect() ([]T, error)
}

type bufferInputStream[T any] struct {
	inputStream[T]
	buffer chan T
	err    error
}

func NewBufferInputStream[T any](bufferSize int) BufferInputStreamer[T] {
	ego := &bufferInputStream[T]{
		buffer: make(chan T, bufferSize),
	}
	ego.init(ego)
	return ego
}

func (ego *bufferInputStream[T]) error(err error) {
	ego.err = err
}

func (ego *bufferInputStream[T]) get() (value T, valid bool, err error) {

	if ego.buffer == nil {
		err = errors.New("Buffer is not initialized.")
		return
	}

	value, valid = <-ego.buffer

	if ego.err != nil {
		err = ego.err
	}
	return

}

func (ego *bufferInputStream[T]) Closed() bool {
	return ego.closed && len(ego.buffer) == 0
}

func (ego *bufferInputStream[T]) Write(p ...T) (int, error) {

	if p == nil {
		panic("Input slice is not initialized.")
	}

	if ego.closed {
		return 0, errors.New("The stream is closed.")
	}

	for _, elem := range p {
		ego.buffer <- elem
	}

	return len(p), nil

}

func (ego *bufferInputStream[T]) Close() {
	close(ego.buffer)
	ego.close()
}

type readableOutputStream[T any] struct {
	outputStream[T]
}

func NewReadableOutputStream[T any]() ReadableOutputStreamer[T] {
	ego := &readableOutputStream[T]{}
	ego.init(ego)
	return ego
}

func (ego *readableOutputStream[T]) Read(p []T) (int, error) {

	if p == nil {
		return 0, errors.New("Input slice is not initialized.")
	}

	if ego.closed {
		return 0, errors.New("The stream is closed.")
	}

	n := len(p)

	for i := 0; i < n; i++ {
		value, valid, err := ego.source.get()
		if err != nil || !valid {
			return i, err
		}
		p[i] = value
		if ego.source.Closed() {
			ego.close()
			break
		}
	}

	return n, nil

}

func (ego *readableOutputStream[T]) Collect() ([]T, error) {

	if ego.closed {
		return nil, errors.New("The stream is closed.")
	}

	output := make([]T, 0)

	for true {
		value, valid, err := ego.source.get()
		if err != nil || !valid {
			return output, err
		}
		output = append(output, value)
		if ego.source.Closed() {
			break
		}
	}

	ego.close()
	return output, nil

}
