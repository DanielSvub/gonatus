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

func NewInputStream[T comparable]() *InputStream[T] {
	return &InputStream[T]{}
}

type OutputStream[T comparable] struct {
	Stream
	source InputStreamer[T]
}

func NewOutputStream[T comparable]() *OutputStream[T] {
	return &OutputStream[T]{}
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
