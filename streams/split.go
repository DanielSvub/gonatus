package streams

type SplitStreamer[T any] interface {
	OutputStreamer[T]
	positive() InputStreamer[T]
	negative() InputStreamer[T]
}

type splitStream[T comparable] struct {
	stream
	source         InputStreamer[T]
	positiveStream BufferInputStreamer[T]
	negativeStream BufferInputStreamer[T]
	condition      func(e T) bool
}

func NewSplitStream[T comparable](bufferSize int, condition func(e T) bool) SplitStreamer[T] {
	ego := &splitStream[T]{
		positiveStream: NewBufferInputStream[T](bufferSize),
		negativeStream: NewBufferInputStream[T](bufferSize),
		condition:      condition,
	}
	ego.init(ego)
	return ego
}

func (ego *splitStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
	go ego.doFilter()
}

func (ego *splitStream[T]) positive() InputStreamer[T] {
	return ego.positiveStream
}

func (ego *splitStream[T]) negative() InputStreamer[T] {
	return ego.negativeStream
}

func (ego *splitStream[T]) doFilter() {

	for true {
		value, valid, err := ego.source.Get()
		if err != nil {
			ego.positiveStream.error(err)
			ego.negativeStream.error(err)
			break
		}
		if valid {
			if ego.condition(value) {
				ego.positiveStream.Write(value)
			} else {
				ego.negativeStream.Write(value)
			}
		}
		if ego.source.Closed() {
			break
		}
	}

	ego.close()
	ego.positiveStream.Close()
	ego.negativeStream.Close()

}
