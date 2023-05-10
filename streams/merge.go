package streams

import "errors"

type MergeStreamer[T any] interface {
	OutputStreamer[T]
	true() InputStreamer[T]
	false() InputStreamer[T]
	Close()
}

type RRMergeStream[T comparable] struct {
	Stream[T]
	sources   []InputStreamer[T]
	currIndex int
	autoclose bool
}

func NewRRMergeStream[T comparable](autoclose bool) *RRMergeStream[T] {
	ego := &RRMergeStream[T]{
		autoclose: autoclose,
	}
	ego.init(ego)
	return ego
}

func (ego *RRMergeStream[T]) setSource(s InputStreamer[T]) {
	ego.sources = append(ego.sources, s)
}

func (ego *RRMergeStream[T]) unsetSource(s InputStreamer[T]) {
	for i, source := range ego.sources {
		if source == s {
			ego.sources = append(ego.sources[:i], ego.sources[i+1:]...)
			break
		}
	}
}

func (ego *RRMergeStream[T]) get() (T, error) {

	if len(ego.sources) == 0 {
		return *new(T), errors.New("The stream is closed.")
	}
	val, err := ego.sources[ego.currIndex].get()

	if ego.sources[ego.currIndex].Closed() {
		ego.unsetSource(ego.sources[ego.currIndex])
		if len(ego.sources) == 0 {
			if ego.autoclose {
				ego.closed = true
			}
			return val, nil
		}
	}

	if ego.currIndex == len(ego.sources)-1 {
		ego.currIndex = 0
	} else {
		ego.currIndex++
	}

	if err != nil {
		return *new(T), err
	}

	return val, nil
}

func (ego *RRMergeStream[T]) Close() {
	if ego.autoclose {
		panic("Cannot close explicitly, autoclose is active.")
	}
	ego.closed = true
}

func (ego *RRMergeStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	return pipe[T](ego, s)
}

func (ego *RRMergeStream[T]) Split(s SplitStreamer[T]) (trueStream InputStreamer[T], falseStream InputStreamer[T]) {
	return split[T](ego, s)
}

func (ego *RRMergeStream[T]) Duplicate(s DuplicationStreamer[T]) (stream1 InputStreamer[T], stream2 InputStreamer[T]) {
	return duplicate[T](ego, s)
}