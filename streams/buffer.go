package streams

import (
	"errors"

	"github.com/SpongeData-cz/gonatus"
)

type BufferInputStreamer[T comparable] interface {
	InputStreamer[T]
	get() (T, error)
	Write(p ...T) (n int, err error)
	Close()
}

type ReadableOutputStreamer[T comparable] interface {
	OutputStreamer[T]
	Read(p []T) (n int, err error)
}

type BufferInputStream[T comparable] struct {
	InputStream[T]
	buffer []T
}

func NewBufferInputStream[T comparable](conf gonatus.Conf) *BufferInputStream[T] {
	ego := &BufferInputStream[T]{buffer: make([]T, 0)}
	ego.Stream.Init(ego, conf)
	return ego
}

func (ego *BufferInputStream[T]) get() (T, error) {

	if ego.buffer == nil {
		return *new(T), errors.New("Buffer is not initialized!\n")
	} else if len(ego.buffer) == 0 {
		return *new(T), errors.New("Buffer is empty!\n")
	}

	elem := ego.buffer[0]
	ego.buffer = ego.buffer[1:]

	return elem, nil
}

func (ego *BufferInputStream[T]) Write(p ...T) (int, error) {

	if p == nil {
		return 0, errors.New("Input slice is not initialized!\n")
	}

	n := len(p)
	ego.buffer = append(ego.buffer, p...)

	return n, nil
}

func (ego *BufferInputStream[T]) Close() {
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
		return 0, errors.New("Input slice is not initialized!\n")
	}

	n := len(p)

	for i := 0; i < n; i++ {
		val, err := ego.source.get()
		if err != nil {
			return 0, err
		}
		p[i] = val
	}

	return n, nil
}
