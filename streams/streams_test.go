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
		if ndo.Run() != nil {
			t.Error("Problem with exporting to file.")
		}

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
		if ndo.Run() != nil {
			t.Error("Problem with exporting to file.")
		}

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

	t.Run("ndjsonAppend", func(t *testing.T) {

		appendF, err := os.Create("fixtures/append.ndjson")
		if err != nil {
			t.Error("Problem with creating a file.")
		}
		s := "{\"data\":{\"name\":\"Bob\", \"id\": 420}, \"type\":\"weirdo\"}\n"
		appendF.WriteString(s)

		ndi := NewNdjsonInputStream("fixtures/example.ndjson")
		ndo := NewNdjsonOutputStream("fixtures/append.ndjson", FileAppend)

		ndi.Pipe(ndo)
		if ndo.Run() != nil {
			t.Error("Problem with exporting to file.")
		}

		origF, err := os.Open("fixtures/example.ndjson")
		if err != nil {
			t.Error("Problem with opening a file.")
		}

		copyF, err := os.Open("fixtures/append.ndjson")
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

		if len(origFConfs)+1 != len(copyFConfs) {
			t.Error("Different number of elements.")
		}

		i := rand.Intn(len(origFConfs))

		if origFConfs[i].Get("type").(string) != copyFConfs[i+1].Get("type").(string) {
			t.Error("The value doesn't match.")
		}

		origF.Close()
		copyF.Close()

		err = os.Remove("fixtures/append.ndjson")
		if err != nil {
			t.Error("Problem with removing a file.")
		}

	})

	t.Run("ndjsonEmpty", func(t *testing.T) {

		nds := NewNdjsonInputStream("fixtures/empty.ndjson")
		os := NewReadableOutputStream[gonatus.Conf]()

		nds.Pipe(os)

		result, err := os.Collect()
		if err != nil || len(result) != 0 {
			t.Error("Collecting the results was unsuccessful.")
		}

	})

}

func TestAdditional(t *testing.T) {

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

	t.Run("transformDup", func(t *testing.T) {

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

	t.Run("filterDup", func(t *testing.T) {

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

	t.Run("mergeDup", func(t *testing.T) {

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
}
