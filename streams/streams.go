package streams

import "github.com/SpongeData-cz/gonatus"

type Streamer[T comparable] interface {
	gonatus.Gobjecter
	Closed() bool
}

type InputStreamer[T comparable] interface {
	Streamer[T]
	get() (T, error)
	Pipe(dest OutputStreamer[T]) InputStreamer[T]
}

type OutputStreamer[T comparable] interface {
	Streamer[T]
	setSource(InputStreamer[T])
}

type TransformStreamer[T comparable] interface {
	InputStreamer[T]
	OutputStreamer[T]
}

type Stream[T comparable] struct {
	gonatus.Gobject
	closed bool
}

func (ego *Stream[T]) Closed() bool {
	return ego.closed
}

type InputStream[T comparable] struct {
	Stream[T]
}

func (ego *InputStream[T]) get() (T, error) {
	panic("Not implemented.")
}

func (ego *InputStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	s.setSource(ego.Ptr().(InputStreamer[T]))
	ts, hasOutput := s.(TransformStreamer[T])
	if hasOutput {
		return ts
	}
	return nil
}

type OutputStream[T comparable] struct {
	Stream[T]
	source InputStreamer[T]
}

func (ego *OutputStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
}

type TransformStream[T comparable] struct {
	Stream[T]
	source    InputStreamer[T]
	Transform func(e T) T
}

func NewTransformStream[T comparable](conf gonatus.Conf) *TransformStream[T] {
	ego := &TransformStream[T]{}
	ego.Init(ego, conf)
	return ego
}

func (ego *TransformStream[T]) get() (T, error) {
	val, err := ego.source.get()
	if err != nil {
		if ego.source.Closed() {
			ego.closed = true
		}
		return *new(T), err
	}
	return ego.Transform(val), nil
}

func (ego *TransformStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
}

func (ego *TransformStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	s.setSource(ego.Ptr().(InputStreamer[T]))
	ts, hasOutput := s.(TransformStreamer[T])
	if hasOutput {
		return ts
	}
	return nil
}
