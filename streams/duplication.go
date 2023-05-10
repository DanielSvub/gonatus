package streams

type DuplicationStreamer[T any] interface {
	OutputStreamer[T]
	first() InputStreamer[T]
	second() InputStreamer[T]
}

type DuplicationStream[T comparable] struct {
	Stream[T]
	source  InputStreamer[T]
	stream1 BufferInputStreamer[T]
	stream2 BufferInputStreamer[T]
}

func NewDuplicationStream[T comparable](bufferSize int) *DuplicationStream[T] {
	ego := &DuplicationStream[T]{
		stream1: NewBufferInputStream[T](bufferSize),
		stream2: NewBufferInputStream[T](bufferSize),
	}
	ego.init(ego)
	return ego
}

func (ego *DuplicationStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
	go ego.duplicate()
}

func (ego *DuplicationStream[T]) first() InputStreamer[T] {
	return ego.stream1
}

func (ego *DuplicationStream[T]) second() InputStreamer[T] {
	return ego.stream2
}

func (ego *DuplicationStream[T]) duplicate() {

	for true {
		val, err := ego.source.get()
		check(err)
		ego.stream1.Write(val)
		ego.stream2.Write(val)
		if ego.source.Closed() {
			break
		}
	}

	ego.closed = true
	ego.stream1.Close()
	ego.stream2.Close()

}
