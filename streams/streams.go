package streams

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

func (ego *InputStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	return pipe[T](ego, s)
}

func (ego *InputStream[T]) Split(s SplitStreamer[T]) (trueStream InputStreamer[T], falseStream InputStreamer[T]) {
	return split[T](ego, s)
}

func (ego *InputStream[T]) Duplicate(s DuplicationStreamer[T]) (stream1 InputStreamer[T], stream2 InputStreamer[T]) {
	return duplicate[T](ego, s)
}

type OutputStream[T any] struct {
	Stream[T]
	source InputStreamer[T]
}

func (ego *OutputStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
}

func check(err error) {
	if err != nil {
		panic(err)
	}
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

func duplicate[T any](ego InputStreamer[T], s DuplicationStreamer[T]) (InputStreamer[T], InputStreamer[T]) {
	s.setSource(ego.ptr().(InputStreamer[T]))
	return s.first(), s.second()
}
