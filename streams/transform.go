package streams

import "errors"

/*
Two-sided stream, transforms the data with the given transformation function.

Extends:
  - InputStreamer,
  - OutputStreamer.

Type parameters:
  - T - input type,
  - U - output type.
*/
type TransformStreamer[T any, U any] interface {
	InputStreamer[U]
	OutputStreamer[T]
}

/*
Transform stream.

Extends:
  - stream.

Implements:
  - InputStreamer,
  - OutputStreamer.

Type parameters:
  - T - input type,
  - U - output type.
*/
type transformStream[T any, U any] struct {
	stream
	source    InputStreamer[T]
	piped     bool
	transform func(e T) U // transformation function
}

/*
Transform stream constructor.

Parameters:
  - transform - transformation function.

Type parameters:
  - T - input type,
  - U - output type.

Returns:
  - pointer to the created transform stream.
*/
func NewTransformStream[T any, U any](transform func(e T) U) TransformStreamer[T, U] {
	ego := &transformStream[T, U]{
		transform: transform,
	}
	ego.init(ego)
	return ego
}

func (ego *transformStream[T, U]) Get() (value U, valid bool, err error) {
	if ego.source == nil {
		return *new(U), false, errors.New("The stream is not attached.")
	}
	val, valid, err := ego.source.Get()
	if ego.source.Closed() {
		ego.close()
	}
	if valid {
		value = ego.transform(val)
	}
	return
}

func (ego *transformStream[T, U]) setSource(s InputStreamer[T]) {
	if ego.source != nil {
		panic("The stream is already attached.")
	}
	ego.source = s
}

func (ego *transformStream[T, U]) Pipe(s OutputStreamer[U]) InputStreamer[U] {
	if ego.piped {
		panic("The stream is already piped.")
	}
	ego.piped = true
	return pipe[U](ego, s)
}

func (ego *transformStream[T, U]) Split(s SplitStreamer[U]) (positiveStream InputStreamer[U], negativeStream InputStreamer[U]) {
	if ego.piped {
		panic("The stream is already piped.")
	}
	ego.piped = true
	return split[U](ego, s)
}

func (ego *transformStream[T, U]) Duplicate(s DuplicationStreamer[U]) (stream1 InputStreamer[U], stream2 InputStreamer[U]) {
	if ego.piped {
		panic("The stream is already piped.")
	}
	ego.piped = true
	return duplicate[U](ego, s)
}
