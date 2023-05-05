package streams

import (
	"errors"

	"github.com/SpongeData-cz/gonatus"
)

type BufferInputStreamer[T comparable] interface {
	InputStreamer[T]
	Write(p ...T) (n int, err error)
	Close()
}

type ReadableOutputStreamer[T comparable] interface {
	OutputStreamer[T]
	Read(p []T) (n int, err error)
	Collect() ([]T, error)
}

type BufferInputStream[T comparable] struct {
	InputStream[T]
	buffer chan T
}

func NewBufferInputStream[T comparable](conf gonatus.Conf) *BufferInputStream[T] {
	ego := &BufferInputStream[T]{buffer: make(chan T, 100)}
	ego.Stream.Init(ego, conf)
	return ego
}

func (ego *BufferInputStream[T]) get() (T, error) {

	if ego.buffer == nil {
		panic("Buffer is not initialized.")
	}
	value, valid := <-ego.buffer

	if valid {
		return value, nil
	} else {
		return *new(T), errors.New("The stream is closed.")
	}
}

func (ego *BufferInputStream[T]) Write(p ...T) (int, error) {

	if p == nil {
		panic("Input slice is not initialized.")
	}

	if ego.Closed() {
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

type ReadableOutputStream[T comparable] struct {
	OutputStream[T]
}

func NewReadableOutputStream[T comparable](conf gonatus.Conf) *ReadableOutputStream[T] {
	ego := &ReadableOutputStream[T]{}
	ego.Stream.Init(ego, conf)
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
		if ego.source.Closed() {
			ego.closed = true
			return i, errors.New("The stream is closed.")
		}
		if err != nil {
			return i, err
		}
		p[i] = val
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
		if ego.source.Closed() {
			break
		}
		if err != nil {
			return output, err
		}
		output = append(output, val)
	}

	ego.closed = true
	return output, nil
}
