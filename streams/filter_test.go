package streams_test

import (
	"math/rand"
	"testing"

	. "github.com/SpongeData-cz/gonatus/streams"
)

func TestFilter(t *testing.T) {
	t.Run("filter", func(t *testing.T) {

		is := NewBufferInputStream[int](10)
		fs := NewFilterStream(func(x int) bool {
			return x <= 5
		})
		os := NewReadableOutputStream[int]()

		is.Write(1, 6, 2, 7, 3, 8, 4, 9, 5, 10)
		is.Close()
		is.Pipe(fs).Pipe(os)

		result, err := os.Collect()
		if err != nil || len(result) != 5 {
			t.Error("Collecting the filtered results was unsuccessful.")
		}

	})

	t.Run("filterSplit", func(t *testing.T) {

		is := NewBufferInputStream[int](12)
		fs := NewFilterStream(func(x int) bool {
			return x <= 5
		})
		ss := NewSplitStream(6, func(x int) bool {
			return x <= 2
		})
		ost := NewReadableOutputStream[int]()
		osf := NewReadableOutputStream[int]()

		is.Write(0, 12, 1, 6, 2, 7, 3, 8, 4, 9, 5, 10)
		is.Close()
		is.Pipe(fs)

		trueS, falseS := fs.Split(ss)
		trueS.Pipe(ost)
		falseS.Pipe(osf)

		result, err := ost.Collect()
		if err != nil || len(result) != 3 {
			t.Error("Collecting the filtered and split results was unsuccessful.")
		}
		result, err = osf.Collect()
		if err != nil || len(result) != 3 {
			t.Error("Collecting the filtered and split results was unsuccessful.")
		}

	})

	t.Run("filterDuplication", func(t *testing.T) {

		is := NewBufferInputStream[int](10)
		fs := NewFilterStream(func(x int) bool {
			return x <= 5
		})
		ds := NewDuplicationStream[int](5)
		os1 := NewReadableOutputStream[int]()
		os2 := NewReadableOutputStream[int]()

		is.Write(1, 6, 2, 7, 3, 8, 4, 9, 5, 10)
		is.Close()
		is.Pipe(fs)

		s1, s2 := fs.Duplicate(ds)
		s1.Pipe(os1)
		s2.Pipe(os2)

		result1, err := os1.Collect()
		if err != nil || len(result1) != 5 {
			t.Error("Collecting the filtered and duplicated results was unsuccessful.")
		}
		result2, err := os2.Collect()
		if err != nil || len(result2) != 5 {
			t.Error("Collecting the filtered and duplicated results was unsuccessful.")
		}

		i := rand.Intn(5)
		if result1[i] != result2[i] {
			t.Error("The values doesn't match.")
		}

	})

	t.Run("errFilter", func(t *testing.T) {
		fs := NewFilterStream(func(x int) bool {
			return x <= 5
		})
		os := NewReadableOutputStream[int]()
		fs.Pipe(os)
		p := make([]int, 4)
		_, err := os.Read(p)
		if err == nil {
			t.Error("Can be read even if it has no input data stream.")
		}
	})

	t.Run("panicFilter", func(t *testing.T) {

		testSetSource := func() {
			is1 := NewBufferInputStream[int](5)
			is2 := NewBufferInputStream[int](5)
			fs := NewFilterStream(func(x int) bool {
				return x <= 5
			})
			is1.Pipe(fs)
			is2.Pipe(fs)
		}

		testPipe := func() {
			fs := NewFilterStream(func(x int) bool {
				return x <= 5
			})
			os1 := NewReadableOutputStream[int]()
			os2 := NewReadableOutputStream[int]()
			fs.Pipe(os1)
			fs.Pipe(os2)
		}

		testSplit := func() {
			fs := NewFilterStream(func(x int) bool {
				return x <= 5
			})
			ss := NewSplitStream(6, func(x int) bool {
				return x <= 2
			})
			os1 := NewReadableOutputStream[int]()
			fs.Pipe(os1)
			fs.Split(ss)
		}

		testDuplicate := func() {
			fs := NewFilterStream(func(x int) bool {
				return x <= 5
			})
			ds := NewDuplicationStream[int](5)
			os1 := NewReadableOutputStream[int]()
			fs.Pipe(os1)
			fs.Duplicate(ds)
		}

		shouldPanic(t, testSetSource)
		shouldPanic(t, testPipe)
		shouldPanic(t, testSplit)
		shouldPanic(t, testDuplicate)

	})

}
