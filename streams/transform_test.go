package streams_test

import (
	"sync"
	"testing"

	. "github.com/SpongeData-cz/gonatus/streams"
)

func TestTransform(t *testing.T) {

	t.Run("TransformAsyncParallel", func(t *testing.T) {

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
				t.Error("Collecting the results transformed in parallel was unsuccessful.")
			}
		}

		go write()
		go read()
		wg.Wait()

	})

	t.Run("transformDuplication", func(t *testing.T) {

		is := NewBufferInputStream[int](10)
		ts := NewTransformStream(func(x int) int {
			return x
		})
		ds := NewDuplicationStream[int](10)
		os1 := NewReadableOutputStream[int]()
		os2 := NewReadableOutputStream[int]()

		is.Write(1, 6, 2, 7, 3, 8, 4, 9, 10, 5)
		is.Close()
		is.Pipe(ts)

		s1, s2 := ts.Duplicate(ds)
		s1.Pipe(os1)
		s2.Pipe(os2)

		result, err := os1.Collect()
		if err != nil || len(result) != 10 {
			t.Error("Collecting the transformed and duplicated results was unsuccessful.")
		}
		result, err = os2.Collect()
		if err != nil || len(result) != 10 {
			t.Error("Collecting the transformed and duplicated results was unsuccessful.")
		}

	})

	t.Run("transformSplit", func(t *testing.T) {

		is := NewBufferInputStream[int](10)
		ts := NewTransformStream(func(x int) int {
			return x
		})
		ss := NewSplitStream(5, func(x int) bool {
			return x <= 5
		})
		ost := NewReadableOutputStream[int]()
		osf := NewReadableOutputStream[int]()

		is.Write(1, 6, 2, 7, 3, 8, 4, 9, 10, 5)
		is.Close()
		is.Pipe(ts)
		trueS, falseS := ts.Split(ss)
		trueS.Pipe(ost)
		falseS.Pipe(osf)

		result, err := ost.Collect()
		if err != nil || len(result) != 5 {
			t.Error("Collecting the transformed and split results was unsuccessful.")
		}

		result, err = osf.Collect()
		if err != nil || len(result) != 5 {
			t.Error("Collecting the transformed and split results was unsuccessful.")
		}

	})

	t.Run("errTransform", func(t *testing.T) {
		ts := NewTransformStream(func(x int) int {
			return x
		})
		os := NewReadableOutputStream[int]()
		ts.Pipe(os)
		p := make([]int, 4)
		_, err := os.Read(p)
		if err == nil {
			t.Error("Can be read even if it has no input data stream.")
		}
	})

	t.Run("panicTransform", func(t *testing.T) {

		testSetSource := func() {
			is1 := NewBufferInputStream[int](5)
			is2 := NewBufferInputStream[int](5)
			ts := NewTransformStream(func(x int) int {
				return x
			})
			is1.Pipe(ts)
			is2.Pipe(ts)
		}

		testPipe := func() {
			ts := NewTransformStream(func(x int) int {
				return x
			})
			os1 := NewReadableOutputStream[int]()
			os2 := NewReadableOutputStream[int]()
			ts.Pipe(os1)
			ts.Pipe(os2)
		}

		testSplit := func() {
			ts := NewTransformStream(func(x int) int {
				return x
			})
			ss := NewSplitStream(6, func(x int) bool {
				return x <= 2
			})
			os1 := NewReadableOutputStream[int]()
			ts.Pipe(os1)
			ts.Split(ss)
		}

		testDuplicate := func() {
			ts := NewTransformStream(func(x int) int {
				return x
			})
			ds := NewDuplicationStream[int](5)
			os1 := NewReadableOutputStream[int]()
			ts.Pipe(os1)
			ts.Duplicate(ds)
		}

		shouldPanic(t, testSetSource)
		shouldPanic(t, testPipe)
		shouldPanic(t, testSplit)
		shouldPanic(t, testDuplicate)

	})

}
