package streams

import (
	"bufio"
	"errors"
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
	Path    private[string]
}

func NewNdjsonStream(conf gonatus.Conf) *NdjsonStream {

	ego := &NdjsonStream{}
	ego.Init(ego, conf)

	file, err := os.Open(ego.Path.value)
	if err != nil {
		panic("The file does not exist.")
	}
	ego.file = file
	ego.scanner = bufio.NewScanner(file)

	return ego
}

func (ego *NdjsonStream) get() (gonatus.Conf, error) {

	if ego.file == nil {
		panic("The file does not exist.")
	}

	if !ego.scanner.Scan() {
		ego.file.Close()
		return nil, errors.New("No lines to read.")
	}

	newConf := gonatus.NewConf("")
	newConf.Unmarshal([]byte(ego.scanner.Text()))

	return newConf, nil
}
