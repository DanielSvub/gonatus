package streams

import "github.com/SpongeData-cz/gonatus"

type private[T any] struct {
	value T
}

func NewPrivate[T any](value T) private[T] {
	return private[T]{value}
}

type Streamer[T any] interface {
	gonatus.Gobjecter
	Closed() bool
}

type InputStreamer[T any] interface {
	Streamer[T]
	get() (T, error)
	Pipe(dest OutputStreamer[T]) InputStreamer[T]
}

type OutputStreamer[T any] interface {
	Streamer[T]
	setSource(InputStreamer[T])
}

type TransformStreamer[T any] interface {
	InputStreamer[T]
	OutputStreamer[T]
}

type Stream[T any] struct {
	gonatus.Gobject
	closed bool
}

func (ego *Stream[T]) Closed() bool {
	return ego.closed
}

type InputStream[T any] struct {
	Stream[T]
}

func (ego *InputStream[T]) get() (T, error) {
	panic("Not implemented.")
}

func pipe[T any](ego InputStreamer[T], s OutputStreamer[T]) InputStreamer[T] {
	s.setSource(ego.Ptr().(InputStreamer[T]))
	ts, hasOutput := s.(InputStreamer[T])
	if hasOutput {
		return ts
	}
	return nil
}

func (ego *InputStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	return pipe[T](ego, s)
}

type OutputStream[T any] struct {
	Stream[T]
	source InputStreamer[T]
}

func (ego *OutputStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
}

type TransformStream[T any] struct {
	Stream[T]
	source    InputStreamer[T]
	Transform func(e T) T
}

func NewTransformStream[T any](conf gonatus.Conf) *TransformStream[T] {
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
	return pipe[T](ego, s)
}
