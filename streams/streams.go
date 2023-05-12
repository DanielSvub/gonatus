package streams

type Streamer interface {
	ptr() Streamer
	init(ptr Streamer)
	close()
	Closed() bool
}

type InputStreamer[T any] interface {
	Streamer
	get() (value T, valid bool, err error)
	Pipe(dest OutputStreamer[T]) InputStreamer[T]
	Split(s SplitStreamer[T]) (trueStream InputStreamer[T], falseStream InputStreamer[T])
	Duplicate(s DuplicationStreamer[T]) (stream1 InputStreamer[T], stream2 InputStreamer[T])
}

type OutputStreamer[T any] interface {
	Streamer
	setSource(InputStreamer[T])
}

type stream struct {
	closed bool
	egoPtr Streamer
}

func (ego *stream) ptr() Streamer {
	return ego.egoPtr
}

func (ego *stream) init(ptr Streamer) {
	ego.egoPtr = ptr
}

func (ego *stream) close() {
	ego.closed = true
}

func (ego *stream) Closed() bool {
	return ego.closed
}

type inputStream[T any] struct {
	stream
}

func (ego *inputStream[T]) get() (value T, valid bool, err error) {
	panic("Not implemented.")
}

func (ego *inputStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	return pipe[T](ego, s)
}

func (ego *inputStream[T]) Split(s SplitStreamer[T]) (trueStream InputStreamer[T], falseStream InputStreamer[T]) {
	return split[T](ego, s)
}

func (ego *inputStream[T]) Duplicate(s DuplicationStreamer[T]) (stream1 InputStreamer[T], stream2 InputStreamer[T]) {
	return duplicate[T](ego, s)
}

type outputStream[T any] struct {
	stream
	source InputStreamer[T]
}

func (ego *outputStream[T]) setSource(s InputStreamer[T]) {
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
