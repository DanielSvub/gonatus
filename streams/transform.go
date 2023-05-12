package streams

type TransformStreamer[T any, U any] interface {
	InputStreamer[U]
	OutputStreamer[T]
}

type transformStream[T any, U any] struct {
	stream
	source    InputStreamer[T]
	transform func(e T) U
}

func NewTransformStream[T any, U any](transform func(e T) U) TransformStreamer[T, U] {
	ego := &transformStream[T, U]{
		transform: transform,
	}
	ego.init(ego)
	return ego
}

func (ego *transformStream[T, U]) get() (value U, valid bool, err error) {
	val, valid, err := ego.source.get()
	if ego.source.Closed() {
		ego.close()
	}
	if valid {
		value = ego.transform(val)
	}
	return
}

func (ego *transformStream[T, U]) setSource(s InputStreamer[T]) {
	ego.source = s
}

func (ego *transformStream[T, U]) Pipe(s OutputStreamer[U]) InputStreamer[U] {
	return pipe[U](ego, s)
}

func (ego *transformStream[T, U]) Split(s SplitStreamer[U]) (positiveStream InputStreamer[U], negativeStream InputStreamer[U]) {
	return split[U](ego, s)
}

func (ego *transformStream[T, U]) Duplicate(s DuplicationStreamer[U]) (stream1 InputStreamer[U], stream2 InputStreamer[U]) {
	return duplicate[U](ego, s)
}
