package streams

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type Streamer[T any] interface {
	ptr() Streamer[T]
	init(ptr Streamer[T])
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
	closed bool
	egoPtr Streamer[T]
}

func (ego *Stream[T]) ptr() Streamer[T] {
	return ego.egoPtr
}

func (ego *Stream[T]) init(ptr Streamer[T]) {
	ego.egoPtr = ptr
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
	s.setSource(ego.ptr().(InputStreamer[T]))
	ts, hasOutput := s.(InputStreamer[T])
	if hasOutput {
		return ts
	}
	return nil
}

func split[T any](ego InputStreamer[T], s SplitStreamer[T]) (InputStreamer[T], InputStreamer[T]) {
	s.setSource(ego.ptr().(InputStreamer[T]))
	return s.true(), s.false()
}

func (ego *InputStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	return pipe[T](ego, s)
}

func (ego *InputStream[T]) Split(s SplitStreamer[T]) (trueStream InputStreamer[T], falseStream InputStreamer[T]) {
	return split[T](ego, s)
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
	transform func(e T) T
}

func NewTransformStream[T any](transform func(e T) T) *TransformStream[T] {
	ego := &TransformStream[T]{
		transform: transform,
	}
	ego.init(ego)
	return ego
}

func (ego *TransformStream[T]) get() (T, error) {
	val, err := ego.source.get()
	if err != nil {
		return *new(T), err
	}
	if ego.source.Closed() {
		ego.closed = true
	}
	return ego.transform(val), nil
}

func (ego *TransformStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
}

func (ego *TransformStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	return pipe[T](ego, s)
}

func (ego *TransformStream[T]) Split(s SplitStreamer[T]) (trueStream InputStreamer[T], falseStream InputStreamer[T]) {
	return split[T](ego, s)
}
