package streams

type TransformStreamer[T any] interface {
	InputStreamer[T]
	OutputStreamer[T]
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

func (ego *TransformStream[T]) Duplicate(s DuplicationStreamer[T]) (stream1 InputStreamer[T], stream2 InputStreamer[T]) {
	return duplicate[T](ego, s)
}
