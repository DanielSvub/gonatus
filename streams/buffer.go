package streams

import "errors"

// implement: get(), write()
type BufferInputStreamer[T comparable] interface {
	InputStreamer[T]
	get() (T, error)
	Write(p ...T) (n int, err error)
}

type BufferInputStream[T comparable] struct {
	InputStream[T]
	buffer []T
}

func NewBufferInputStream[T comparable]() *BufferInputStream[T] {
	return &BufferInputStream[T]{}
}

// implement: Read()
type BufferOutputStreamer[T comparable] interface {
	OutputStreamer[T]
	Read(p []T) (n int, err error)
}

type BufferOutputStream[T comparable] struct {
	OutputStream[T]
	buffer []T
}

func NewBufferOutputStream[T comparable]() *BufferOutputStream[T] {
	return &BufferOutputStream[T]{}
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

	if ego.buffer == nil {
		return 0, errors.New("Buffer is not initialized!\n")
	} else if p == nil {
		return 0, errors.New("Input slice is not initialized!\n")
	}

	n := len(p)
	ego.buffer = append(ego.buffer, p...)

	return n, nil
}

func (ego *BufferOutputStream[T]) Read(p []T) (int, error) {

	if ego.buffer == nil {
		return 0, errors.New("Buffer is not initialized!\n")
	} else if p == nil {
		return 0, errors.New("Input slice is not initialized!\n")
	}

	var n int

	if len(ego.buffer) < len(p) {
		n = len(ego.buffer)
	} else {
		n = len(p)
	}

	for i := 0; i < n; i++ {
		p[i] = ego.buffer[i]
	}

	ego.buffer = ego.buffer[n:]

	return n, nil
}
