package streams

import "errors"

type FilterStreamer[T any] interface {
	InputStreamer[T]
	OutputStreamer[T]
}

type FilterStream[T any] struct {
	Stream[T]
	source InputStreamer[T]
	filter func(e T) bool
}

func NewFilterStream[T any](filter func(e T) bool) *FilterStream[T] {
	ego := &FilterStream[T]{
		filter: filter,
	}
	ego.init(ego)
	return ego
}

func (ego *FilterStream[T]) get() (T, error) {
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

func (ego *FilterStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
}

func (ego *FilterStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	return pipe[T](ego, s)
}

func (ego *FilterStream[T]) Split(s SplitStreamer[T]) (trueStream InputStreamer[T], falseStream InputStreamer[T]) {
	return split[T](ego, s)
}

func (ego *FilterStream[T]) Duplicate(s DuplicationStreamer[T]) (stream1 InputStreamer[T], stream2 InputStreamer[T]) {
	return duplicate[T](ego, s)
}
