package streams

type FilterStreamer[T any] interface {
	InputStreamer[T]
	OutputStreamer[T]
}

type filterStream[T any] struct {
	stream
	source InputStreamer[T]
	filter func(e T) bool
}

func NewFilterStream[T any](filter func(e T) bool) FilterStreamer[T] {
	ego := &filterStream[T]{
		filter: filter,
	}
	ego.init(ego)
	return ego
}

func (ego *filterStream[T]) get() (value T, valid bool, err error) {
	for true {
		value, valid, err = ego.source.get()
		closed := ego.source.Closed()
		if closed {
			ego.close()
		}
		if valid && ego.filter(value) {
			return
		}
		if closed {
			break
		}
	}
	valid = false
	return
}

func (ego *filterStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
}

func (ego *filterStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	return pipe[T](ego, s)
}

func (ego *filterStream[T]) Split(s SplitStreamer[T]) (trueStream InputStreamer[T], falseStream InputStreamer[T]) {
	return split[T](ego, s)
}

func (ego *filterStream[T]) Duplicate(s DuplicationStreamer[T]) (stream1 InputStreamer[T], stream2 InputStreamer[T]) {
	return duplicate[T](ego, s)
}
