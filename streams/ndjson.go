package streams

import (
	"bufio"
	"os"

	"github.com/SpongeData-cz/gonatus"
)

const (
	FileWrite = iota
	FileAppend
)

/*
Source of the ndjson data.

Extends:
  - InputStreamer
*/
type NdjsonInputStreamer interface {
	InputStreamer[gonatus.Conf]
}

/*
Output stream that writes json to the ndjson file.

Extends:
  - OutputStreamer
*/
type NdjsonOutputStreamer interface {
	OutputStreamer[gonatus.Conf]

	/*
		Writes individual items as json to the ndjson file.

		Returns:
		  - error - error, if any occurred.

	*/
	Run() error
}

/*
Ndjson source stream.

Extends:
  - inputStream.

Implements:
  - NdjsonInputStreamer.
*/
type ndjsonInputStream struct {
	inputStream[gonatus.Conf]
	file    *os.File
	scanner *bufio.Scanner
}

/*
Ndjson input stream constructor.

Parameters:
  - path - path to ndjson file.

Type parameters:
  - string

Returns:
  - pointer to the created ndjson input stream.
*/
func NewNdjsonInputStream(path string) NdjsonInputStreamer {

	ego := &ndjsonInputStream{}
	ego.init(ego)

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	ego.file = file
	ego.scanner = bufio.NewScanner(file)

	return ego

}

func (ego *ndjsonInputStream) get() (value gonatus.Conf, valid bool, err error) {

	if ego.file == nil {
		panic("The file does not exist.")
	}

	valid = ego.scanner.Scan()

	if valid {
		value = gonatus.NewConf("")
		value.Unmarshal([]byte(ego.scanner.Text()))
	} else {
		ego.file.Close()
		ego.close()
	}

	return
}

/*
Destination ndjson stream.

Extends:
  - outputStream.

Implements:
  - NdjsonOutputStreamer.
*/
type ndjsonOutputStream struct {
	outputStream[gonatus.Conf]
	file *os.File
}

/*
Ndjson output stream constructor.

Parameters:
  - path - path to ndjson file,
  - mode - mode of how to write to the file.

Type parameters:
  - path - string,
  - mode - int.

Returns:
  - pointer to the created ndjson output stream.
*/
func NewNdjsonOutputStream(path string, mode int) NdjsonOutputStreamer {

	if mode != FileAppend && mode != FileWrite {
		panic("Unknown mode.")
	}

	ego := &ndjsonOutputStream{}
	ego.init(ego)

	var flags int
	if mode == FileWrite {
		flags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	} else {
		flags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	}

	file, err := os.OpenFile(path, flags, 0664)
	if err != nil {
		panic(err)
	}

	ego.file = file

	return ego

}

func (ego *ndjsonOutputStream) Run() error {

	for true {

		value, valid, err := ego.source.get()
		if !valid {
			break
		}

		if err != nil {
			return err
		}
		nd, err := value.Marshal()
		if err != nil {
			return err
		}
		_, err = ego.file.Write(nd)
		if err != nil {
			return err
		}
		_, err = ego.file.WriteString("\n")
		if err != nil {
			return err
		}
		if ego.source.Closed() {
			break
		}

	}

	ego.closed = true
	ego.file.Close()

	return nil

}
