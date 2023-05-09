package streams

import (
	"errors"
)

type BufferInputStreamer[T any] interface {
	InputStreamer[T]
	Write(p ...T) (n int, err error)
	Close()
}

type ReadableOutputStreamer[T any] interface {
	OutputStreamer[T]
	Read(p []T) (n int, err error)
	Collect() ([]T, error)
}

type BufferInputStream[T any] struct {
	InputStream[T]
	buffer chan T
}

func NewBufferInputStream[T any](bufferSize int) *BufferInputStream[T] {
	ego := &BufferInputStream[T]{
		buffer: make(chan T, bufferSize),
	}
	ego.init(ego)
	return ego
}

func (ego *BufferInputStream[T]) get() (T, error) {

	if ego.buffer == nil {
		panic("Buffer is not initialized.")
	}

	value, valid := <-ego.buffer

	if valid {
		return value, nil
	}

	return *new(T), errors.New("Read after channel closing.")
}

func (ego *BufferInputStream[T]) Closed() bool {
	return ego.closed && len(ego.buffer) == 0
}

func (ego *BufferInputStream[T]) Write(p ...T) (int, error) {

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

func (ego *BufferInputStream[T]) Close() {
	close(ego.buffer)
	ego.closed = true
}

type ReadableOutputStream[T any] struct {
	OutputStream[T]
}

func NewReadableOutputStream[T any]() *ReadableOutputStream[T] {
	ego := &ReadableOutputStream[T]{}
	ego.init(ego)
	return ego
}

func (ego *ReadableOutputStream[T]) Read(p []T) (int, error) {

	if p == nil {
		panic("Input slice is not initialized.")
	}

	if ego.closed {
		return 0, errors.New("The stream is closed.")
	}

	n := len(p)

	for i := 0; i < n; i++ {
		val, err := ego.source.get()
		if err != nil {
			return i, err
		}
		p[i] = val
		if ego.source.Closed() {
			ego.closed = true
			break
		}
	}

	return n, nil
}

func (ego *ReadableOutputStream[T]) Collect() ([]T, error) {

	if ego.closed {
		return nil, errors.New("The stream is closed.")
	}

	output := make([]T, 0)

	for true {
		val, err := ego.source.get()
		if err != nil {
			if ego.source.Closed() {
				return output, nil
			}
			return output, err
		}
		output = append(output, val)
		if ego.source.Closed() {
			break
		}
	}

	ego.closed = true
	return output, nil
}
