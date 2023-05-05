package streams_test

import (
	"sync"
	"testing"

	"github.com/SpongeData-cz/gonatus"
	. "github.com/SpongeData-cz/gonatus/streams"
)

func TestStreams(t *testing.T) {

	t.Run("read", func(t *testing.T) {

		result := make([]int, 3)

		is := NewBufferInputStream[int](gonatus.NewConf("NewBufferInputStream").Set(
			gonatus.NewPair("BufferSize", NewPrivate(100)),
		))
		ts := NewTransformStream[int](gonatus.NewConf("TransformStream").Set(
			gonatus.NewPair("Transform", func(x int) int {
				return x * x
			}),
		))
		os := NewReadableOutputStream[int](nil)

		is.Write(1, 2, 3)
		is.Close()
		is.Pipe(ts).Pipe(os)
		n, err := os.Read(result)

		if err != nil || n != 3 || len(result) != 3 {
			t.Error("Reading the results was unsuccessful.")
		}

	})

	t.Run("collect", func(t *testing.T) {

		is := NewBufferInputStream[int](gonatus.NewConf("NewBufferInputStream").Set(
			gonatus.NewPair("BufferSize", NewPrivate(100)),
		))
		ts := NewTransformStream[int](gonatus.NewConf("TransformStream").Set(
			gonatus.NewPair("Transform", func(x int) int {
				return x * x
			}),
		))
		os := NewReadableOutputStream[int](nil)

		is.Write(1, 2, 3)
		is.Close()
		is.Pipe(ts).Pipe(os)

		result, err := os.Collect()
		if err != nil || len(result) != 3 {
			t.Error("Collecting the results was unsuccessful.")
		}

	})

	t.Run("async", func(t *testing.T) {

		is := NewBufferInputStream[int](gonatus.NewConf("NewBufferInputStream").Set(
			gonatus.NewPair("BufferSize", NewPrivate(100)),
		))
		ts := NewTransformStream[int](gonatus.NewConf("TransformStream").Set(
			gonatus.NewPair("Transform", func(x int) int {
				return x + 1
			}),
		))
		os := NewReadableOutputStream[int](nil)

		var wg sync.WaitGroup
		wg.Add(2)

		write := func() {
			defer wg.Done()
			defer is.Close()
			is.Write(make([]int, 1000000)...)
		}

		is.Pipe(ts).Pipe(os)

		read := func() {
			defer wg.Done()
			result, err := os.Collect()
			if err != nil || len(result) != 1000000 {
				t.Error("Collecting the results in parallel was unsuccessful.")
			}
		}

		go write()
		go read()
		wg.Wait()

	})

}
