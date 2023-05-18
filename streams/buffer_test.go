package streams_test

import (
	"sync"
	"testing"

	. "github.com/SpongeData-cz/gonatus/streams"
)

func TestBuffer(t *testing.T) {

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
		os := NewReadableOutputStream[int]()

		var wg sync.WaitGroup
		wg.Add(2)

		write := func() {
			defer wg.Done()
			defer is.Close()
			is.Write(make([]int, 1000000)...)
		}

		is.Pipe(os)

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

	t.Run("errBuffer", func(t *testing.T) {
		is := NewBufferInputStream[int](5)
		os := NewReadableOutputStream[int]()

		is.Write(1, 2, 3)
		is.Close()
		_, err := is.Write(4, 5)
		if err == nil {
			t.Error("Can be written into the stream even though it shouldn't be possible.")
		}
		is.Pipe(os)

		_, err = os.Read(nil)
		if err == nil {
			t.Error("Can read from the stream even if it has nil input slice.")
		}
		_, err = os.Collect()
		if err != nil {
			t.Error("Nothing was collected from the stream.")
		}
		p := make([]int, 5)
		_, err = os.Read(p)
		if err == nil {
			t.Error("Can read the stream even when the stream is closed.")
		}
		_, err = os.Collect()
		if err == nil {
			t.Error("Can collect from the stream even when the stream is closed.")
		}
	})

	t.Run("panicBuffer", func(t *testing.T) {

		testWrite := func() {
			is := NewBufferInputStream[int](5)
			var a []int
			is.Write(a...)
		}

		shouldPanic(t, testWrite)

	})

	t.Run("panicROutput", func(t *testing.T) {

		testPipe := func() {
			is := NewBufferInputStream[int](5)
			os1 := NewReadableOutputStream[int]()
			os2 := NewReadableOutputStream[int]()
			is.Pipe(os1)
			is.Pipe(os2)
		}
		testSplit := func() {
			is := NewBufferInputStream[int](5)
			ss := NewSplitStream(6, func(x int) bool {
				return x <= 2
			})
			os1 := NewReadableOutputStream[int]()
			is.Pipe(os1)
			is.Split(ss)
		}
		testDuplicate := func() {
			is := NewBufferInputStream[int](5)
			ds := NewDuplicationStream[int](5)
			os1 := NewReadableOutputStream[int]()
			is.Pipe(os1)
			is.Duplicate(ds)
		}
		testSetSource := func() {
			is1 := NewBufferInputStream[int](5)
			is2 := NewBufferInputStream[int](5)
			os := NewReadableOutputStream[int]()
			is1.Pipe(os)
			is2.Pipe(os)
		}

		shouldPanic(t, testPipe)
		shouldPanic(t, testSplit)
		shouldPanic(t, testDuplicate)
		shouldPanic(t, testSetSource)

	})

}

func shouldPanic(t *testing.T, f func()) {
	defer func() { recover() }()
	f()
	t.Error("Should have paniced")
}
