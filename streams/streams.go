/*
The package provides data streams with a lazy init capability.

Types of streams:
  - input stream - one-sided stream, reads data from the source,
  - transform stream - two-sided stream, transforms value of each item,
  - filter stream - two-sided stream, discards items not safisfying a condition,
  - split stream - two-sided stream, splits one stream into two,
  - merge stream - two-sided stream, merges two streams into one,
  - duplication stream - two-sided stream, makes two identical streams from a single one,
  - output stream - one-sided stream, terminates the pipe and exports the data.
*/
package streams

/*
A Streamer is the base interface for streams.
*/
type Streamer interface {
	/*
		Acquires an ego pointer.
		Allowes an access to the called struct, instead the nested Streamer.

		Returns:
		  - pointer to the stream.
	*/
	ptr() Streamer

	/*
		Sets the ego pointer.

		Parameters:
		  - pointer to the stream.
	*/
	init(ptr Streamer)

	/*
		Closes the stream.
	*/
	close()

	/*
		Checks whether the stream is closed.

		Returns:
		  - true if the stream is closed, false otherwise.
	*/
	Closed() bool
}

/*
Source of the data.

Extends:
  - Streamer.

Type parameters:
  - T - type of the data.
*/
type InputStreamer[T any] interface {
	Streamer

	/*
		Acquires a next item from the stream.

		Returns:
			- value - the value of the item,
			- valid - true if the value is present, false otherwise,
			- err - error, if any occurred.
	*/
	Get() (value T, valid bool, err error)

	/*
		Attaches the given stream to this one.

		Parameters:
		  - dest - the destination output stream.

		Returns:
		  - the destination stream typed as an input stream.
	*/
	Pipe(dest OutputStreamer[T]) InputStreamer[T]

	/*
		Splits the streams into two.

		Parameters:
		  - s - the split stream.

		Returns:
		  - positiveStream - the stream of the values satisfying the split stream condition,
		  - negativeStream - the stream of the rest.
	*/
	Split(s SplitStreamer[T]) (positiveStream InputStreamer[T], negativeStream InputStreamer[T])

	/*
		Splits the streams into two.

		Parameters:
		  - s - the duplication stream.

		Returns:
		  - stream1 - the stream of the values satisfying the split stream condition,
		  - stream2 - the stream of the rest.
	*/
	Duplicate(s DuplicationStreamer[T]) (stream1 InputStreamer[T], stream2 InputStreamer[T])
}

/*
Possible destination for the data.

Extends:
  - Streamer.

Type parameters:
  - T - type of the data.
*/
type OutputStreamer[T any] interface {
	Streamer

	/*
		Sets the source stream for this stream.

		Parameters:
		  - s - the source stream.
	*/
	setSource(s InputStreamer[T])
}

/*
Base struct for streams.

Implements:
  - Streamer.
*/
type stream struct {
	closed bool     // whether the stream is closed
	egoPtr Streamer // the ego pointer
}

func (ego *stream) ptr() Streamer {
	return ego.egoPtr
}

func (ego *stream) init(ptr Streamer) {
	ego.egoPtr = ptr
}

func (ego *stream) close() {
	ego.closed = true
}

func (ego *stream) Closed() bool {
	return ego.closed
}

/*
Source stream.

Extends:
  - stream.

Implements:
  - InputStreamer.
*/
type inputStream[T any] struct {
	stream
	piped bool
}

func (ego *inputStream[T]) Get() (value T, valid bool, err error) {
	panic("Not implemented.")
}

func (ego *inputStream[T]) Pipe(s OutputStreamer[T]) InputStreamer[T] {
	if ego.piped {
		panic("The stream is already piped.")
	}
	ego.piped = true
	return pipe[T](ego, s)
}

func (ego *inputStream[T]) Split(s SplitStreamer[T]) (trueStream InputStreamer[T], falseStream InputStreamer[T]) {
	if ego.piped {
		panic("The stream is already piped.")
	}
	return split[T](ego, s)
}

func (ego *inputStream[T]) Duplicate(s DuplicationStreamer[T]) (stream1 InputStreamer[T], stream2 InputStreamer[T]) {
	if ego.piped {
		panic("The stream is already piped.")
	}
	return duplicate[T](ego, s)
}

/*
Destination stream.

Extends:
  - stream.

Implements:
  - OutputStreamer.
*/
type outputStream[T any] struct {
	stream
	source InputStreamer[T] // source stream
}

func (ego *outputStream[T]) setSource(s InputStreamer[T]) {
	if ego.source != nil {
		panic("The stream is already attached.")
	}
	ego.source = s
}

/*
Implements the InputStreamer's Pipe method.
*/
func pipe[T any](ego InputStreamer[T], s OutputStreamer[T]) InputStreamer[T] {
	s.setSource(ego.ptr().(InputStreamer[T]))
	ts, hasOutput := s.(InputStreamer[T])
	if hasOutput {
		return ts
	}
	return nil
}

/*
Implements the InputStreamer's Duplicate method.
*/
func split[T any](ego InputStreamer[T], s SplitStreamer[T]) (InputStreamer[T], InputStreamer[T]) {
	s.setSource(ego.ptr().(InputStreamer[T]))
	return s.positive(), s.negative()
}

/*
Implements the InputStreamer's Duplicate method.
*/
func duplicate[T any](ego InputStreamer[T], s DuplicationStreamer[T]) (InputStreamer[T], InputStreamer[T]) {
	s.setSource(ego.ptr().(InputStreamer[T]))
	return s.first(), s.second()
}
