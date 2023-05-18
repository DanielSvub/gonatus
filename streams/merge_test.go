package streams_test

import (
	"math/rand"
	"sync"
	"testing"

	. "github.com/SpongeData-cz/gonatus/streams"
)

func TestMerge(t *testing.T) {

	t.Run("mergeParallel", func(t *testing.T) {

		is1 := NewBufferInputStream[int](5)
		is2 := NewBufferInputStream[int](5)
		ms := NewRRMergeStream[int](true)
		os := NewReadableOutputStream[int]()

		var wg sync.WaitGroup
		wg.Add(3)

		write1 := func() {
			defer wg.Done()
			defer is1.Close()
			is1.Write(1, 3, 5, 7, 9)
		}

		write2 := func() {
			defer wg.Done()
			defer is2.Close()
			is2.Write(2, 4, 6, 8, 10)
		}

		is1.Pipe(ms)
		is2.Pipe(ms)
		ms.Pipe(os)

		read := func() {
			defer wg.Done()
			result, err := os.Collect()
			if err != nil || len(result) != 10 {
				t.Error("Collecting the results from the merge stream was unsuccessful.")
			}
		}

		go write1()
		go write2()
		go read()
		wg.Wait()

	})

	t.Run("mergeClose", func(t *testing.T) {

		is1 := NewBufferInputStream[int](5)
		is2 := NewBufferInputStream[int](5)
		ms := NewRRMergeStream[int](false)
		os := NewReadableOutputStream[int]()

		is1.Write(1, 3, 5, 7, 9)
		is1.Close()
		is2.Write(2, 4, 6, 8, 10)
		is2.Close()

		is1.Pipe(ms)
		is2.Pipe(ms)

		ms.Pipe(os)

		result, err := os.Collect()
		if err != nil {
			ms.Close()
		}
		if len(result) != 10 {
			t.Error("Collecting the merged results was unsuccessful.")
		}
	})

	t.Run("mergeSplit", func(t *testing.T) {

		is1 := NewBufferInputStream[int](5)
		is2 := NewBufferInputStream[int](5)
		ms := NewRRMergeStream[int](true)
		ss := NewSplitStream(5, func(x int) bool {
			return x <= 5
		})
		ost := NewReadableOutputStream[int]()
		osf := NewReadableOutputStream[int]()

		is1.Write(1, 6, 2, 7, 3)
		is1.Close()
		is2.Write(5, 10, 9, 8, 4)
		is2.Close()

		is1.Pipe(ms)
		is2.Pipe(ms)

		trueS, falseS := ms.Split(ss)

		trueS.Pipe(ost)
		falseS.Pipe(osf)

		resultT, err := ost.Collect()
		if err != nil || len(resultT) != 5 {
			t.Error("Collecting the merged and split results was unsuccessful.")
		}
		resultF, err := osf.Collect()
		if err != nil || len(resultF) != 5 {
			t.Error("Collecting the merged and split results was unsuccessful.")
		}

		testTSlice := []int{1, 5, 2, 3, 4}
		testFSlice := []int{6, 10, 9, 7, 8}
		i := rand.Intn(5)

		if resultT[i] != testTSlice[i] || resultF[i] != testFSlice[i] {
			t.Error("The values doesn't match.")
		}

	})

	t.Run("mergeDuplication", func(t *testing.T) {

		is1 := NewBufferInputStream[int](5)
		is2 := NewBufferInputStream[int](5)
		ms := NewRRMergeStream[int](true)
		ds := NewDuplicationStream[int](10)
		os1 := NewReadableOutputStream[int]()
		os2 := NewReadableOutputStream[int]()

		is1.Write(1, 6, 2, 7, 3)
		is1.Close()
		is2.Write(5, 10, 9, 8, 4)
		is2.Close()

		is1.Pipe(ms)
		is2.Pipe(ms)

		s1, s2 := ms.Duplicate(ds)

		s1.Pipe(os1)
		s2.Pipe(os2)

		result1, err := os1.Collect()
		if err != nil || len(result1) != 10 {
			t.Error("Collecting the merged and duplicated results was unsuccessful.")
		}
		result2, err := os2.Collect()
		if err != nil || len(result2) != 10 {
			t.Error("Collecting the merged and duplicated results was unsuccessful.")
		}

		testSlice := []int{1, 5, 6, 10, 2, 9, 7, 8, 3, 4}
		i := rand.Intn(10)

		if result1[i] != testSlice[i] || result2[i] != testSlice[i] {
			t.Error("The values doesn't match.")
		}

	})

	t.Run("panicMerge", func(t *testing.T) {

		testClose := func() {
			ms := NewRRMergeStream[int](true)
			ms.Close()
		}
		testPipe := func() {
			ms := NewRRMergeStream[int](true)
			os1 := NewReadableOutputStream[int]()
			os2 := NewReadableOutputStream[int]()
			ms.Pipe(os1)
			ms.Pipe(os2)
		}
		testSplit := func() {
			ms := NewRRMergeStream[int](true)
			ss := NewSplitStream(6, func(x int) bool {
				return x <= 2
			})
			os1 := NewReadableOutputStream[int]()
			ms.Pipe(os1)
			ms.Split(ss)
		}
		testDuplicate := func() {
			ms := NewRRMergeStream[int](true)
			ds := NewDuplicationStream[int](5)
			os1 := NewReadableOutputStream[int]()
			ms.Pipe(os1)
			ms.Duplicate(ds)
		}

		shouldPanic(t, testClose)
		shouldPanic(t, testPipe)
		shouldPanic(t, testSplit)
		shouldPanic(t, testDuplicate)

	})

}
