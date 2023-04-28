package structures

import (
	"errors"

	"github.com/SpongeData-cz/gonatus"
)

type Appender[T comparable] interface {
	Append(items ...T)
}

type Reader[T comparable] interface {
	Read(p []T) (n int, err error)
}

type Seeker interface {
	Seek(offset int, whence int) (int, error)
}

type Closer interface {
	Close() error
}

type Taper[T comparable] interface {
	gonatus.Gobjecter
	Seeker
	Closer
	Reader[T]
	Appender[T]
	Filter(dest Taper[T], fn func(T) bool) error
	Closed() bool
}

type RAMTape[T comparable] struct {
	gonatus.Gobject
	Slice  []T
	Offset int
	closed bool
}

func NewRAMTape[T comparable](conf gonatus.Conf) *RAMTape[T] {
	ego := &RAMTape[T]{}
	ego.Init(ego, conf)
	return ego
}

/*
Interface for a frame.

Extends:
  - Gobjecter.

Type parameters:
  - T - type of the frame elements.
*/
type Framer[T comparable] interface {
	gonatus.Gobjecter
	Next() error
	Load() ([]T, error)
}

/*
Structure representing the frame - sliding window on the tape.

Implements:
  - Framer.

Type parameters:
  - T - type of the frame elements.
*/
type Frame[T comparable] struct {
	gonatus.Gobject
	Tape     Taper[T]
	Offset   int // Start of frame
	Size     int // Actual number of loaded elements
	Capacity int // Maximum number of elements that can be loaded
}

/*
Creates a new frame.

Parameters:
  - conf - Gonatus configuration structure.

Type parameters:
  - T - type of the frame elements.

Returns:
  - pointer to the new frame.
*/
func NewFrame[T comparable](conf gonatus.Conf) *Frame[T] {
	ego := &Frame[T]{}
	ego.Init(ego, conf)
	return ego
}

/*
Shifts the frame offset by the number of the lastly loaded elements.
Increases the offset by size and sets size to 0.

Returns:
  - error, if no elements were loaded since last usage of this method.
*/
func (ego *Frame[T]) Next() error {

	if ego.Size == 0 {
		return errors.New("No values loaded since last call.\n")
	}

	ego.Offset += ego.Size
	ego.Size = 0
	return nil
}

/*
Loads the maximum possible amount of elements into the slice and returns it.
Also updates the size to number of actually loaded elements (0 <= size <= capacity).

Returns:
  - slice of loaded elements,
  - error, if any occurred.
*/
func (ego *Frame[T]) Load() ([]T, error) {

	s := make([]T, ego.Capacity)

	size, err := ego.Tape.Read(s)
	if err != nil {
		return nil, err
	}

	ego.Size = size
	return s[:size], nil
}
