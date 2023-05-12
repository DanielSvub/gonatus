package streams

type DuplicationStreamer[T any] interface {
	OutputStreamer[T]
	first() InputStreamer[T]
	second() InputStreamer[T]
}

type duplicationStream[T comparable] struct {
	stream
	source  InputStreamer[T]
	stream1 BufferInputStreamer[T]
	stream2 BufferInputStreamer[T]
}

func NewDuplicationStream[T comparable](bufferSize int) DuplicationStreamer[T] {
	ego := &duplicationStream[T]{
		stream1: NewBufferInputStream[T](bufferSize),
		stream2: NewBufferInputStream[T](bufferSize),
	}
	ego.init(ego)
	return ego
}

func (ego *duplicationStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
	go ego.duplicate()
}

func (ego *duplicationStream[T]) first() InputStreamer[T] {
	return ego.stream1
}

func (ego *duplicationStream[T]) second() InputStreamer[T] {
	return ego.stream2
}

func (ego *duplicationStream[T]) duplicate() {

	for true {
		value, valid, err := ego.source.get()
		check(err)
		if valid {
			ego.stream1.Write(value)
			ego.stream2.Write(value)
		}
		if ego.source.Closed() {
			break
		}
	}

	ego.close()
	ego.stream1.Close()
	ego.stream2.Close()

}
