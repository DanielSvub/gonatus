package streams_test

import (
	"sync"
	"testing"

	. "github.com/SpongeData-cz/gonatus/streams"
)

func TestSplit(t *testing.T) {

	t.Run("splitParallel", func(t *testing.T) {

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
				t.Error("Collecting the results from the split stream was unsuccessful.")
			}
		}

		readF := func() {
			defer wg.Done()
			result, err := osf.Collect()
			if err != nil || len(result) != 5 {
				t.Error("Collecting the results from the split stream was unsuccessful.")
			}
		}

		go write()
		go readT()
		go readF()
		wg.Wait()

	})

}

func TestDuplication(t *testing.T) {

	t.Run("duplicationParallel", func(t *testing.T) {

		is := NewBufferInputStream[int](5)
		ds := NewDuplicationStream[int](5)
		ost := NewReadableOutputStream[int]()
		osf := NewReadableOutputStream[int]()

		var wg sync.WaitGroup
		wg.Add(3)

		write := func() {
			defer wg.Done()
			defer is.Close()
			is.Write(1, 2, 3, 4, 5)
		}

		s1, s2 := is.Duplicate(ds)
		s1.Pipe(ost)
		s2.Pipe(osf)

		read1 := func() {
			defer wg.Done()
			result, err := ost.Collect()
			if err != nil || len(result) != 5 {
				t.Error("Collecting the results from the duplication stream was unsuccessful.")
			}
		}

		read2 := func() {
			defer wg.Done()
			result, err := osf.Collect()
			if err != nil || len(result) != 5 {
				t.Error("Collecting the results from the duplication stream was unsuccessful.")
			}
		}

		go write()
		go read1()
		go read2()
		wg.Wait()

	})

}
