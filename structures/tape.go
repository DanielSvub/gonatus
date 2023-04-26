package structures

import (
	"io"

	"github.com/SpongeData-cz/gonatus"
)

type Appender[T comparable] interface {
	Append(item []T)
}

type Reader[T comparable] interface {
	Read(p []T) (n int, err error)
}

type Taper[T comparable] interface {
	gonatus.Gobjecter
	io.Seeker
	io.Closer
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

type Framer[T comparable] interface {
	gonatus.Gobjecter
	Next() error
	Load() ([]T, error)
}

type Frame[T comparable] struct {
	gonatus.Gobject
	Taper[T]
	offset int
	size   int
}
