package streams_test

import (
	"bufio"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/SpongeData-cz/gonatus"
	. "github.com/SpongeData-cz/gonatus/streams"
)

func TestBasic(t *testing.T) {

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

}
func TestParallel(t *testing.T) {

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

	t.Run("asyncTs", func(t *testing.T) {

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

	t.Run("duplication", func(t *testing.T) {

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

	t.Run("merge", func(t *testing.T) {

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

}

func TestNdjson(t *testing.T) {

	t.Run("ndjsonInput", func(t *testing.T) {

		nds := NewNdjsonInputStream("fixtures/example.ndjson")
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

	t.Run("ndjsonOutput", func(t *testing.T) {

		ndi := NewNdjsonInputStream("fixtures/example.ndjson")
		ndo := NewNdjsonOutputStream("fixtures/exampleCopy.ndjson", FileWrite)

		ndi.Pipe(ndo)

		origF, err := os.Open("fixtures/example.ndjson")
		if err != nil {
			t.Error("Problem with opening a file.")
		}

		copyF, err := os.Open("fixtures/exampleCopy.ndjson")
		if err != nil {
			t.Error("Problem with opening a file.")
		}

		origFScanner := bufio.NewScanner(origF)
		copyFScanner := bufio.NewScanner(copyF)

		origFConfs := make([]gonatus.Conf, 0)
		for origFScanner.Scan() {
			newConf := gonatus.NewConf("")
			newConf.Unmarshal([]byte(origFScanner.Text()))
			origFConfs = append(origFConfs, newConf)
		}

		copyFConfs := make([]gonatus.Conf, 0)
		for copyFScanner.Scan() {
			newConf := gonatus.NewConf("")
			newConf.Unmarshal([]byte(copyFScanner.Text()))
			copyFConfs = append(copyFConfs, newConf)
		}

		if len(origFConfs) != len(copyFConfs) {
			t.Error("Different number of elements.")
		}

		i := rand.Intn(len(origFConfs))

		if origFConfs[i].Get("type").(string) != copyFConfs[i].Get("type").(string) {
			t.Error("The value doesn't match.")
		}

		origF.Close()
		copyF.Close()

		err = os.Remove("fixtures/exampleCopy.ndjson")
		if err != nil {
			t.Error("Problem with removing a file.")
		}

	})

	t.Run("ndjsonOutputTs", func(t *testing.T) {

		ndi := NewNdjsonInputStream("fixtures/example.ndjson")
		ts := NewTransformStream(func(x gonatus.Conf) gonatus.Conf {
			id := x.Get("data").(map[string]any)["id"].(float64)
			x.Get("data").(map[string]any)["id"] = id + 1
			return x
		})
		ndo := NewNdjsonOutputStream("fixtures/exampleModified.ndjson", FileWrite)

		ndi.Pipe(ts).Pipe(ndo)

		origF, err := os.Open("fixtures/example.ndjson")
		if err != nil {
			t.Error("Problem with opening a file.")
		}

		modF, err := os.Open("fixtures/exampleModified.ndjson")
		if err != nil {
			t.Error("Problem with opening a file.")
		}

		origFScanner := bufio.NewScanner(origF)
		modFScanner := bufio.NewScanner(modF)

		origFConfs := make([]gonatus.Conf, 0)
		for origFScanner.Scan() {
			newConf := gonatus.NewConf("")
			newConf.Unmarshal([]byte(origFScanner.Text()))
			origFConfs = append(origFConfs, newConf)
		}

		modFConfs := make([]gonatus.Conf, 0)
		for modFScanner.Scan() {
			newConf := gonatus.NewConf("")
			newConf.Unmarshal([]byte(modFScanner.Text()))
			modFConfs = append(modFConfs, newConf)
		}

		if len(origFConfs) != len(modFConfs) {
			t.Error("Different number of elements.")
		}

		i := rand.Intn(len(origFConfs))

		val1 := origFConfs[i].Get("data").(map[string]any)["id"].(float64)
		val2 := modFConfs[i].Get("data").(map[string]any)["id"].(float64)

		if (val1 + 1) != val2 {
			t.Error("The value doesn't match.")
		}

		origF.Close()
		modF.Close()

		err = os.Remove("fixtures/exampleModified.ndjson")
		if err != nil {
			t.Error("Problem with removing a file.")
		}

	})

}
