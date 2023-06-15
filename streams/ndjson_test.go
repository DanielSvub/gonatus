package streams_test

import (
	"bufio"
	"math/rand"
	"os"
	"testing"

	"github.com/SpongeData-cz/gonatus"
	. "github.com/SpongeData-cz/gonatus/streams"
)

func TestNdjson(t *testing.T) {

	t.Run("ndjsonInput", func(t *testing.T) {

		nds := NewNdjsonInputStream("fixtures/example.ndjson")
		ts := NewTransformStream(func(x gonatus.DynamicConf) gonatus.DynamicConf {
			return x
		})
		os := NewReadableOutputStream[gonatus.DynamicConf]()

		nds.Pipe(ts).Pipe(os)

		result, err := os.Collect()
		if err != nil || len(result) != 6 {
			t.Error("Collecting the results was unsuccessful.")
		}

		val := result[2]["data"].(gonatus.DynamicConf)["name"]
		if val != "Arnold" {
			t.Error("Name is not matching.")
		}

	})

	t.Run("ndjsonOutputWrite", func(t *testing.T) {

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

		origFConfs := make([]gonatus.DynamicConf, 0)
		for origFScanner.Scan() {
			newConf := gonatus.NewConf("")
			newConf.Unmarshal([]byte(origFScanner.Text()))
			origFConfs = append(origFConfs, newConf)
		}

		copyFConfs := make([]gonatus.DynamicConf, 0)
		for copyFScanner.Scan() {
			newConf := gonatus.NewConf("")
			newConf.Unmarshal([]byte(copyFScanner.Text()))
			copyFConfs = append(copyFConfs, newConf)
		}

		if len(origFConfs) != len(copyFConfs) {
			t.Error("Different number of elements.")
		}

		i := rand.Intn(len(origFConfs))

		if origFConfs[i]["type"].(string) != copyFConfs[i]["type"].(string) {
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

		ndi := NewNdjsonInputStream("fixtures/example.ndjson")
		ts := NewTransformStream(func(x gonatus.DynamicConf) gonatus.DynamicConf {
			id := x["data"].(gonatus.DynamicConf)["id"].(float64)
			x["data"].(gonatus.DynamicConf)["id"] = id + 1
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

		origFConfs := make([]gonatus.DynamicConf, 0)
		for origFScanner.Scan() {
			newConf := gonatus.NewConf("")
			newConf.Unmarshal([]byte(origFScanner.Text()))
			origFConfs = append(origFConfs, newConf)
		}

		modFConfs := make([]gonatus.DynamicConf, 0)
		for modFScanner.Scan() {
			newConf := gonatus.NewConf("")
			newConf.Unmarshal([]byte(modFScanner.Text()))
			modFConfs = append(modFConfs, newConf)
		}

		if len(origFConfs) != len(modFConfs) {
			t.Error("Different number of elements.")
		}

		i := rand.Intn(len(origFConfs))

		val1 := origFConfs[i]["data"].(gonatus.DynamicConf)["id"].(float64)
		val2 := modFConfs[i]["data"].(gonatus.DynamicConf)["id"].(float64)

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

		origFConfs := make([]gonatus.DynamicConf, 0)
		for origFScanner.Scan() {
			newConf := gonatus.NewConf("")
			newConf.Unmarshal([]byte(origFScanner.Text()))
			origFConfs = append(origFConfs, newConf)
		}

		copyFConfs := make([]gonatus.DynamicConf, 0)
		for copyFScanner.Scan() {
			newConf := gonatus.NewConf("")
			newConf.Unmarshal([]byte(copyFScanner.Text()))
			copyFConfs = append(copyFConfs, newConf)
		}

		if len(origFConfs)+1 != len(copyFConfs) {
			t.Error("Different number of elements.")
		}

		i := rand.Intn(len(origFConfs))

		if origFConfs[i]["type"].(string) != copyFConfs[i+1]["type"].(string) {
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
		os := NewReadableOutputStream[gonatus.DynamicConf]()

		nds.Pipe(os)

		result, err := os.Collect()
		if err != nil || len(result) != 0 {
			t.Error("Collecting the results was unsuccessful.")
		}

	})

	t.Run("errNdjsonNonExistFile", func(t *testing.T) {

		nds := NewNdjsonInputStream("fixtures/nonExist.ndjson")
		os := NewReadableOutputStream[gonatus.DynamicConf]()

		nds.Pipe(os)

		res, err := os.Collect()
		if err == nil || len(res) != 0 {
			t.Error("This stream is reading my hand.")
		}

	})
	t.Run("errNdjsonClosed", func(t *testing.T) {
		ndi := NewNdjsonInputStream("fixtures/example.ndjson")
		ndo := NewNdjsonOutputStream("fixtures/exampleCopy.ndjson", FileWrite)

		ndi.Pipe(ndo)
		if ndo.Run() != nil {
			t.Error("Problem with exporting to file.")
		}
		if ndo.Run() == nil {
			t.Error("The stream was not closed properly.")
		}

		err := os.Remove("fixtures/exampleCopy.ndjson")
		if err != nil {
			t.Error("Problem with removing a file.")
		}

	})

	t.Run("errNdjsonWrongPath", func(t *testing.T) {
		ndo := NewNdjsonOutputStream("wrong\\path/nonExist.ndjson", FileAppend)

		if ndo.Run() == nil {
			t.Error("Path magically deciphered and alien file created")
		}

	})

	t.Run("panicNdjson", func(t *testing.T) {

		testWrongMode := func() {
			ndo := NewNdjsonOutputStream("fixtures/example.ndjson", 4)
			ndo.Closed()
		}

		shouldPanic(t, testWrongMode)

	})

}
