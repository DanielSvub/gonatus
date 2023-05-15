package streams

import (
	"errors"
)

/*
Buffered source of data.

Extends:
  - InputStreamer.

Type parameters:
  - T - type of the data.
*/
type BufferInputStreamer[T any] interface {
	InputStreamer[T]

	/*
		Sets an error. In case the error needs to be propagated.

		Parameters:
		  - err - error to be set.
	*/
	error(err error)

	/*
		Writes individual elements from the slice to the stream buffer.

		Parameters:
		  - p - A slice of elements to be written.

		Returns:
		  - n - number of written elements,
		  - err - error, if any occurred.
	*/
	Write(p ...T) (n int, err error)

	/*
		Closes the stream.
	*/
	Close()
}

/*
A output stream that can be read into the slice.

Extends:
  - OutputStreamer.

Type parameters:
  - T - type of the data.
*/
type ReadableOutputStreamer[T any] interface {
	OutputStreamer[T]

	/*
		Reads a maximum of len(p) elements from the stream and writes them to the p.

		Parameters:
		  - p - A slice where elements from the stream are read.

		Returns:
		  - n - number of read elements,
		  - err - error, if any occurred.
	*/
	Read(p []T) (n int, err error)

	/*
		Reads all the elements from the stream and returns them.

		Returns:
		  - []T - slice in which all elements from the stream are,
		  - error - error, if any occurred.
	*/
	Collect() ([]T, error)
}

/*
Buffered source stream.

Extends:
  - inputStream.

Implements:
  - BufferInputStreamer.
*/
type bufferInputStream[T any] struct {
	inputStream[T]
	buffer chan T
	err    error
}

/*
Buffer input stream constructor.

Parameters:
  - bufferSize - size of the buffer.

Returns:
  - pointer to the created buffer input stream.
*/
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

/*
Destination readable stream.

Extends:
  - outputStream.

Implements:
  - ReadableOutputStreamer.
*/
type readableOutputStream[T any] struct {
	outputStream[T]
}

/*
Readable output stream constructor.

Returns:
  - pointer to the created readable output stream.
*/
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
