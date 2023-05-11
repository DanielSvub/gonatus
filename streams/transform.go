package streams

type TransformStreamer[T any] interface {
	InputStreamer[T]
	OutputStreamer[T]
}

type transformStream[T any] struct {
	stream[T]
	source    InputStreamer[T]
	transform func(e T) T
}

func NewTransformStream[T any](transform func(e T) T) TransformStreamer[T] {
	ego := &transformStream[T]{
		transform: transform,
	}
	ego.init(ego)
	return ego
}

func (ego *transformStream[T]) get() (value T, valid bool, err error) {
	value, valid, err = ego.source.get()
	if ego.source.Closed() {
		ego.close()
	}
	if valid {
		value = ego.transform(value)
	}
	return
}

func (ego *transformStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
}

func (ego *transformStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	return pipe[T](ego, s)
}

func (ego *transformStream[T]) Split(s SplitStreamer[T]) (trueStream InputStreamer[T], falseStream InputStreamer[T]) {
	return split[T](ego, s)
}

func (ego *transformStream[T]) Duplicate(s DuplicationStreamer[T]) (stream1 InputStreamer[T], stream2 InputStreamer[T]) {
	return duplicate[T](ego, s)
}
