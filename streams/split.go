package streams

import "github.com/SpongeData-cz/gonatus"

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
	Filter      func(e T) bool
	BufferSize  private[int]
}

func NewSplitStream[T comparable](conf gonatus.Conf) *SplitStream[T] {
	ego := &SplitStream[T]{}
	ego.Init(ego, conf)
	ego.trueStream = NewBufferInputStream[T](gonatus.NewConf("NewBufferInputStream").Set(
		gonatus.NewPair("BufferSize", NewPrivate(ego.BufferSize)),
	))
	ego.falseStream = NewBufferInputStream[T](gonatus.NewConf("NewBufferInputStream").Set(
		gonatus.NewPair("BufferSize", NewPrivate(ego.BufferSize)),
	))
	return ego
}

func (ego *SplitStream[T]) setSource(s InputStreamer[T]) {
	ego.source = s
	go ego.doFilter()
}

func (ego SplitStream[T]) true() InputStreamer[T] {
	return ego.trueStream
}

func (ego SplitStream[T]) false() InputStreamer[T] {
	return ego.falseStream
}

func (ego SplitStream[T]) doFilter() {

	for true {
		val, err := ego.source.get()
		if err != nil {
			if ego.source.Closed() {
				break
			}
			panic(err)
		}
		if ego.Filter(val) {
			ego.trueStream.Write(val)
		} else {
			ego.falseStream.Write(val)
		}
	}

	ego.closed = true
	ego.trueStream.Close()
	ego.falseStream.Close()

}
