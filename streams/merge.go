package streams

import "errors"

type MergeStreamer[T any] interface {
	InputStreamer[T]
	OutputStreamer[T]
	Close()
}

type rrMergeStream[T comparable] struct {
	stream
	sources   []InputStreamer[T]
	currIndex int
	autoclose bool
}

func NewRRMergeStream[T comparable](autoclose bool) MergeStreamer[T] {
	ego := &rrMergeStream[T]{
		autoclose: autoclose,
	}
	ego.init(ego)
	return ego
}

func (ego *rrMergeStream[T]) setSource(s InputStreamer[T]) {
	ego.sources = append(ego.sources, s)
}

func (ego *rrMergeStream[T]) unsetSource(s InputStreamer[T]) {
	for i, source := range ego.sources {
		if source == s {
			ego.sources = append(ego.sources[:i], ego.sources[i+1:]...)
			break
		}
	}
	if ego.currIndex == len(ego.sources) {
		ego.currIndex = 0
	}
}

func (ego *rrMergeStream[T]) get() (value T, valid bool, err error) {

	if len(ego.sources) == 0 {
		return *new(T), false, errors.New("The stream has no sources.")
	}

	value, valid, err = ego.sources[ego.currIndex].get()

	if ego.sources[ego.currIndex].Closed() {
		ego.unsetSource(ego.sources[ego.currIndex])
		if len(ego.sources) == 0 {
			if ego.autoclose {
				ego.close()
			}
		}
		return
	}

	if ego.currIndex == len(ego.sources)-1 {
		ego.currIndex = 0
	} else {
		ego.currIndex++
	}

	return

}

func (ego *rrMergeStream[T]) Close() {
	if ego.autoclose {
		panic("Cannot close explicitly, autoclose is active.")
	}
	ego.close()
}

func (ego *rrMergeStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	return pipe[T](ego, s)
}

func (ego *rrMergeStream[T]) Split(s SplitStreamer[T]) (positiveStream InputStreamer[T], negativeStream InputStreamer[T]) {
	return split[T](ego, s)
}

func (ego *rrMergeStream[T]) Duplicate(s DuplicationStreamer[T]) (stream1 InputStreamer[T], stream2 InputStreamer[T]) {
	return duplicate[T](ego, s)
}
