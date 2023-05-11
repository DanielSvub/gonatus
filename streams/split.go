package streams

type SplitStreamer[T any] interface {
	OutputStreamer[T]
	true() InputStreamer[T]
	false() InputStreamer[T]
}

type splitStream[T comparable] struct {
	stream[T]
	source      InputStreamer[T]
	trueStream  BufferInputStreamer[T]
	falseStream BufferInputStreamer[T]
	filter      func(e T) bool
}

func NewSplitStream[T comparable](bufferSize int, filter func(e T) bool) SplitStreamer[T] {
	ego := &splitStream[T]{
		trueStream:  NewBufferInputStream[T](bufferSize),
		falseStream: NewBufferInputStream[T](bufferSize),
		filter:      filter,
	}
	ego.init(ego)
	return ego
}

func (ego *splitStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
	go ego.doFilter()
}

func (ego *splitStream[T]) true() InputStreamer[T] {
	return ego.trueStream
}

func (ego *splitStream[T]) false() InputStreamer[T] {
	return ego.falseStream
}

func (ego *splitStream[T]) doFilter() {

	for true {
		value, valid, err := ego.source.get()
		check(err)
		if valid && ego.filter(value) {
			ego.trueStream.Write(value)
		} else {
			ego.falseStream.Write(value)
		}
		if ego.source.Closed() {
			break
		}
	}

	ego.close()
	ego.trueStream.Close()
	ego.falseStream.Close()

}
