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

type NdjsonInputStreamer interface {
	InputStreamer[gonatus.Conf]
}

type NdjsonInputStream struct {
	InputStream[gonatus.Conf]
	file    *os.File
	scanner *bufio.Scanner
}

func NewNdjsonInputStream(path string) *NdjsonInputStream {

	ego := &NdjsonInputStream{}
	ego.init(ego)

	file, err := os.Open(path)
	check(err)
	ego.file = file
	ego.scanner = bufio.NewScanner(file)

	if !ego.scanner.Scan() {
		ego.file.Close()
		ego.closed = true
		panic("File is empty.")
	}

	return ego
}

func (ego *NdjsonInputStream) get() (gonatus.Conf, error) {

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

type NdjsonOutputStreamer interface {
	OutputStreamer[gonatus.Conf]
}

type NdjsonOutputStream struct {
	OutputStream[gonatus.Conf]
	file *os.File
}

func NewNdjsonOutputStream(path string, mode int) *NdjsonOutputStream {

	if mode != FileAppend && mode != FileWrite {
		panic("Wrong mode.")
	}

	ego := &NdjsonOutputStream{}
	ego.init(ego)

	var flags int
	if mode == FileWrite {
		flags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	} else {
		flags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	}

	file, err := os.OpenFile(path, flags, 0664)
	check(err)

	ego.file = file

	return ego
}

func (ego *NdjsonOutputStream) setSource(s InputStreamer[gonatus.Conf]) {
	ego.source = s
	ego.export()
}

func (ego *NdjsonOutputStream) export() {
	for true {
		val, err := ego.source.get()
		check(err)
		nd, err := val.Marshal()
		check(err)
		_, err = ego.file.Write(nd)
		check(err)
		_, err = ego.file.WriteString("\n")
		check(err)
		if ego.source.Closed() {
			break
		}
	}

	ego.closed = true
	ego.file.Close()
}
