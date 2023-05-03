package streams

// implement: get(), write()
type BufferInputStreamer[T comparable] interface {
	InputStreamer[T]
	Write(p ...T) (n int, err error)
}

// implement: Read()
type BufferOutputStreamer[T comparable] interface {
	OutputStreamer[T]
	Read(p []T) (n int, err error)
}
