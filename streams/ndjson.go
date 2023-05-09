package streams

import (
	"bufio"
	"os"

	"github.com/SpongeData-cz/gonatus"
)

type NdjsonStreamer interface {
	InputStreamer[gonatus.Conf]
}

type NdjsonStream struct {
	InputStream[gonatus.Conf]
	file    *os.File
	scanner *bufio.Scanner
}

func NewNdjsonStream(path string) *NdjsonStream {

	ego := &NdjsonStream{}
	ego.init(ego)

	file, err := os.Open(path)
	if err != nil {
		panic("The file does not exist.")
	}
	ego.file = file
	ego.scanner = bufio.NewScanner(file)

	if !ego.scanner.Scan() {
		ego.file.Close()
		ego.closed = true
		panic("File is empty.")
	}

	return ego
}

func (ego *NdjsonStream) get() (gonatus.Conf, error) {

	if ego.file == nil {
		panic("The file does not exist.")
	}

	newConf := gonatus.NewConf("")
	newConf.Unmarshal([]byte(ego.scanner.Text()))

	if !ego.scanner.Scan() {
		ego.file.Close()
		ego.closed = true
	}

	return newConf, nil
}
