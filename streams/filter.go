package streams

import "errors"

type FilterStreamer[T any] interface {
	InputStreamer[T]
	OutputStreamer[T]
}

type filterStream[T any] struct {
	stream[T]
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

func (ego *filterStream[T]) get() (T, error) {
	for true {
		val, err := ego.source.get()
		if err != nil {
			return *new(T), err
		}
		if ego.source.Closed() {
			ego.closed = true
		}
		if ego.filter(val) {
			return val, nil
		}
		if ego.closed {
			break
		}
	}
	return *new(T), errors.New("No more values satisfying the filter.")
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
