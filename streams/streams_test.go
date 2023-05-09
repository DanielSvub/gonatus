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

		is := NewBufferInputStream[int](3)
		ts := NewTransformStream(func(x int) int {
			return x * x
		})
		os := NewReadableOutputStream[int]()

		is.Write(1, 2, 3)
		is.Close()
		is.Pipe(ts).Pipe(os)
		n, err := os.Read(result)

		if err != nil || n != 3 || len(result) != 3 {
			t.Error("Reading the results was unsuccessful.")
		}

	})

	t.Run("collect", func(t *testing.T) {

		is := NewBufferInputStream[int](3)
		ts := NewTransformStream(func(x int) int {
			return x * x
		})
		os := NewReadableOutputStream[int]()

		is.Write(1, 2, 3)
		is.Close()
		is.Pipe(ts).Pipe(os)

		result, err := os.Collect()
		if err != nil || len(result) != 3 {
			t.Error("Collecting the results was unsuccessful.")
		}

	})

	t.Run("async", func(t *testing.T) {

		is := NewBufferInputStream[int](100)
		ts := NewTransformStream(func(x int) int {
			return x + 1
		})
		os := NewReadableOutputStream[int]()

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

	t.Run("split", func(t *testing.T) {

		is := NewBufferInputStream[int](10)
		ss := NewSplitStream(5, func(x int) bool {
			return x <= 5
		})
		ost := NewReadableOutputStream[int]()
		osf := NewReadableOutputStream[int]()

		var wg sync.WaitGroup
		wg.Add(3)

		write := func() {
			defer wg.Done()
			defer is.Close()
			is.Write(1, 6, 2, 7, 3, 8, 4, 9, 10, 5)
		}

		trueS, falseS := is.Split(ss)
		trueS.Pipe(ost)
		falseS.Pipe(osf)

		readT := func() {
			defer wg.Done()
			result, err := ost.Collect()
			if err != nil || len(result) != 5 {
				t.Error("Collecting the results in parallel was unsuccessful.")
			}
		}

		readF := func() {
			defer wg.Done()
			result, err := osf.Collect()
			if err != nil || len(result) != 5 {
				t.Error("Collecting the results in parallel was unsuccessful.")
			}
		}

		go write()
		go readT()
		go readF()
		wg.Wait()

	})

	t.Run("ndjson", func(t *testing.T) {

		nds := NewNdjsonStream("fixtures/example.ndjson")
		ts := NewTransformStream(func(x gonatus.Conf) gonatus.Conf {
			return x
		})
		os := NewReadableOutputStream[gonatus.Conf]()

		nds.Pipe(ts).Pipe(os)

		result, err := os.Collect()
		if err != nil || len(result) != 6 {
			t.Error("Collecting the results was unsuccessful.")
		}

		val := result[2].Get("data").(map[string]any)["name"]
		if val != "Arnold" {
			t.Error("Name is not matching.")
		}

	})

}
