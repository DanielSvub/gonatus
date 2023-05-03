package streams

type Streamer interface {
	Close() error
	Closed() bool
}

type InputStreamer[T comparable] interface {
	Streamer
	get() (T, error)
	Pipe(dest OutputStreamer[T]) InputStreamer[T]
}

type OutputStreamer[T comparable] interface {
	Streamer
	setSource(InputStreamer[T]) error
}

type TransformStreamer[T comparable] interface {
	InputStreamer[T]
	OutputStreamer[T]
}

type Stream struct {
	closed bool
}

func (ego *Stream) Close() error {
	ego.closed = true
	return nil
}

func (ego *Stream) Closed() bool {
	return ego.closed
}

type InputStream[T comparable] struct {
	Stream
}

func (ego *InputStream[T]) get() (T, error) {
	panic("Not implemented.")
}

func (ego *InputStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	s.setSource(ego)
	ts, hasOutput := s.(TransformStreamer[T])
	if hasOutput {
		return ts
	}
	return nil
}

type OutputStream[T comparable] struct {
	Stream
	source InputStreamer[T]
}

func (ego *OutputStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
}

type TransformStream[T comparable] struct {
	InputStream[T]
	OutputStream[T]
	transform func(e T) T
}

func NewTransformStream[T comparable](transform func(e T) T) *TransformStream[T] {
	return &TransformStream[T]{
		transform: transform,
	}
}

func (ego *TransformStream[T]) get() (T, error) {
	val, err := ego.InputStream.get()
	if err != nil {
		return *new(T), err
	}
	return ego.transform(val), nil
}
