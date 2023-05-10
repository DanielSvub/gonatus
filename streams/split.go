package streams

type SplitStreamer[T any] interface {
	OutputStreamer[T]
	true() InputStreamer[T]
	false() InputStreamer[T]
}

type SplitStream[T comparable] struct {
	Stream[T]
	source      InputStreamer[T]
	trueStream  BufferInputStreamer[T]
	falseStream BufferInputStreamer[T]
	filter      func(e T) bool
}

func NewSplitStream[T comparable](bufferSize int, filter func(e T) bool) *SplitStream[T] {
	ego := &SplitStream[T]{
		trueStream:  NewBufferInputStream[T](bufferSize),
		falseStream: NewBufferInputStream[T](bufferSize),
		filter:      filter,
	}
	ego.init(ego)
	return ego
}

func (ego *SplitStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
	go ego.doFilter()
}

func (ego *SplitStream[T]) true() InputStreamer[T] {
	return ego.trueStream
}

func (ego *SplitStream[T]) false() InputStreamer[T] {
	return ego.falseStream
}

func (ego *SplitStream[T]) doFilter() {

	for true {
		val, err := ego.source.get()
		check(err)
		if ego.filter(val) {
			ego.trueStream.Write(val)
		} else {
			ego.falseStream.Write(val)
		}
		if ego.source.Closed() {
			break
		}
	}

	ego.closed = true
	ego.trueStream.Close()
	ego.falseStream.Close()

}
