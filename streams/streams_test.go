package streams_test

import (
	"testing"

	"github.com/SpongeData-cz/gonatus"
	. "github.com/SpongeData-cz/gonatus/streams"
)

func TestStreams(t *testing.T) {

	t.Run("read", func(t *testing.T) {

		result := make([]int, 3)

		is := NewBufferInputStream[int](nil)
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

		is := NewBufferInputStream[int](nil)
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

}
