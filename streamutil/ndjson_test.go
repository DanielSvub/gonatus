package streamutil_test

import (
	. "github.com/SpongeData-cz/gonatus/streamutil"
	"github.com/SpongeData-cz/stream"

	"bufio"
	"encoding/json"
	"math/rand"
	"os"
	"testing"
)

func TestNdjson(t *testing.T) {

	type data struct {
		Name string
		Id   int
	}

	type person struct {
		Data data
		Type string
	}

	t.Run("ndjsonInput", func(t *testing.T) {

		nds := NewNdjsonInput[person]("fixtures/example.ndjson")

		result, err := nds.Collect()
		if err != nil || len(result) != 6 {
			println(err.Error())
			t.Error("Collecting the results was unsuccessful.")
		}

		if result[2].Data.Name != "Arnold" {
			t.Error("Name is not matching.")
		}

	})

	t.Run("ndjsonOutputWrite", func(t *testing.T) {

		ndi := NewNdjsonInput[person]("fixtures/example.ndjson")
		ndo := NewNdjsonOutput[person]("fixtures/exampleCopy.ndjson", FileWrite)

		ndi.Pipe(ndo)
		if ndo.Run(nil) != nil {
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

		origFConfs := make([]person, 0)
		for origFScanner.Scan() {
			var new person
			json.Unmarshal([]byte(origFScanner.Text()), &new)
			origFConfs = append(origFConfs, new)
		}

		copyFConfs := make([]person, 0)
		for copyFScanner.Scan() {
			var new person
			json.Unmarshal([]byte(copyFScanner.Text()), &new)
			copyFConfs = append(copyFConfs, new)
		}

		if len(origFConfs) != len(copyFConfs) {
			t.Error("Different number of elements.")
		}

		i := rand.Intn(len(origFConfs))

		if origFConfs[i].Type != copyFConfs[i].Type {
			t.Error("The value doesn't match.")
		}

		origF.Close()
		copyF.Close()

		err = os.Remove("fixtures/exampleCopy.ndjson")
		if err != nil {
			t.Error("Problem with removing a file.")
		}

	})

	t.Run("ndjsonOutputTransform", func(t *testing.T) {

		ndi := NewNdjsonInput[person]("fixtures/example.ndjson")
		ts := stream.NewTransformer(func(x person) person {
			x.Data.Id++
			return x
		})
		ndo := NewNdjsonOutput[person]("fixtures/exampleModified.ndjson", FileWrite)

		ndi.Pipe(ts).(stream.Producer[person]).Pipe(ndo)
		if ndo.Run(nil) != nil {
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

		origFConfs := make([]person, 0)
		for origFScanner.Scan() {
			var new person
			json.Unmarshal([]byte(origFScanner.Text()), &new)
			origFConfs = append(origFConfs, new)
		}

		modFConfs := make([]person, 0)
		for modFScanner.Scan() {
			var new person
			json.Unmarshal([]byte(modFScanner.Text()), &new)
			modFConfs = append(modFConfs, new)
		}

		if len(origFConfs) != len(modFConfs) {
			t.Error("Different number of elements.")
		}

		i := rand.Intn(len(origFConfs))

		val1 := origFConfs[i].Data.Id
		val2 := modFConfs[i].Data.Id

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

		ndi := NewNdjsonInput[person]("fixtures/example.ndjson")
		ndo := NewNdjsonOutput[person]("fixtures/append.ndjson", FileAppend)

		ndi.Pipe(ndo)
		if ndo.Run(nil) != nil {
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

		origFConfs := make([]person, 0)
		for origFScanner.Scan() {
			var new person
			if err := json.Unmarshal([]byte(origFScanner.Text()), &new); err != nil {
				t.Error(err)
			}
			origFConfs = append(origFConfs, new)
		}

		copyFConfs := make([]person, 0)
		for copyFScanner.Scan() {
			var new person
			if err := json.Unmarshal([]byte(copyFScanner.Text()), &new); err != nil {
				t.Error(err)
			}
			copyFConfs = append(copyFConfs, new)
		}

		if len(origFConfs)+1 != len(copyFConfs) {
			t.Error("Different number of elements.")
		}

		i := rand.Intn(len(origFConfs))

		if origFConfs[i].Type != copyFConfs[i+1].Type {
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

		nds := NewNdjsonInput[person]("fixtures/empty.ndjson")

		result, err := nds.Collect()
		if err != nil || len(result) != 0 {
			t.Error("Collecting the results was unsuccessful.")
		}

	})

	t.Run("errNdjsonNonExistFile", func(t *testing.T) {

		nds := NewNdjsonInput[person]("fixtures/nonExist.ndjson")

		res, err := nds.Collect()
		if err == nil || len(res) != 0 {
			t.Error("This stream is reading my hand.")
		}

	})
	t.Run("errNdjsonClosed", func(t *testing.T) {
		ndi := NewNdjsonInput[person]("fixtures/example.ndjson")
		ndo := NewNdjsonOutput[person]("fixtures/exampleCopy.ndjson", FileWrite)

		ndi.Pipe(ndo)
		if ndo.Run(nil) != nil {
			t.Error("Problem with exporting to file.")
		}
		if ndo.Run(nil) == nil {
			t.Error("The stream was not closed properly.")
		}

		err := os.Remove("fixtures/exampleCopy.ndjson")
		if err != nil {
			t.Error("Problem with removing a file.")
		}

	})

	t.Run("errNdjsonWrongPath", func(t *testing.T) {
		ndo := NewNdjsonOutput[person]("wrong\\path/nonExist.ndjson", FileAppend)

		if ndo.Run(nil) == nil {
			t.Error("Path magically deciphered and alien file created")
		}

	})

	t.Run("panicNdjson", func(t *testing.T) {

		testWrongMode := func() {
			NewNdjsonOutput[person]("fixtures/example.ndjson", 4)
		}

		shouldPanic(t, testWrongMode)

	})

}

func shouldPanic(t *testing.T, f func()) {
	defer func() { recover() }()
	f()
	t.Error("Should have paniced")
}
