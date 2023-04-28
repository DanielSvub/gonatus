package structures

import (
	"fmt"
	"io"
	"testing"

	gts "github.com/SpongeData-cz/gonatus"
)

var tape Taper[gts.Conf]
var confs = []gts.Conf{
	gts.NewConf("test1"),
	gts.NewConf("test2"),
	gts.NewConf("test3"),
	gts.NewConf("test4")}

func TestAppendAndRead(t *testing.T) {
	tape = NewRAMTape[gts.Conf](gts.NewConf("RAMTape"))
	for i, v := range confs {
		v.Set(gts.NewPair("a", i+1))
		tape.Append(v)
	}
	tape.Append(confs...)

	buf := make([]gts.Conf, 2*len(confs))
	tape.Seek(0, 0)
	n, err := tape.Read(buf)
	if err != nil {
		t.Error(err)
	}
	if n != 2*len(confs) {
		t.Error("Wrong number of elements red", n)
	}

	for i := 0; i < len(confs); i++ {
		if buf[i] != confs[i] || buf[i+len(confs)] != confs[i] {
			t.Error("Invalid size of tape or order of data")
		}
	}
	fmt.Printf("tape: %v\n", *(tape.(*RAMTape[gts.Conf])))
}

func TestSeek(t *testing.T) {
	tape = NewRAMTape[gts.Conf](gts.NewConf("RAMTape"))
	for i, v := range confs {
		v.Set(gts.NewPair("a", i+1))
		tape.Append(v)
	}
	tape.Append(confs...)

	buf := make([]gts.Conf, 1)
	sn, err := tape.Seek(len(confs), io.SeekStart)
	if err != nil {
		t.Error(err)
	}
	if sn != len(confs) {
		t.Error("Seek problem", sn)
	}
	n, err := tape.Read(buf)
	if err != nil {
		t.Error(err)
	}
	if n != 1 {
		t.Error("Wrong number of elements red", n)
	}
	if buf[0] != confs[0] {
		t.Error("Wrong element seeked")
	}

	//seek extremes
	sn, err = tape.Seek(0, io.SeekStart)
	if sn != 0 {
		t.Error("Seek problem", sn)
	}
	n, err = tape.Read(buf)
	if err != nil {
		t.Error(err)
	}
	if n != 1 {
		t.Error("Wrong number of elements red", n)
	}
	if buf[0] != confs[0] {
		t.Error("Wrong element seeked")
	}

	sn, err = tape.Seek(3, io.SeekStart)
	if sn != 3 {
		t.Error("Seek problem", sn)
	}
	n, err = tape.Read(buf)
	if err != nil {
		t.Error(err)
	}
	if n != 1 {
		t.Error("Wrong number of elements red", n)
	}
	if buf[0] != confs[3] {
		t.Error("Wrong element seeked")
	}
	sn, err = tape.Seek(0, io.SeekCurrent)
	if sn != 4 {
		t.Error("Seek problem", sn)
	}
	n, err = tape.Read(buf)
	if err != nil {
		t.Error(err)
	}
	if n != 1 {
		t.Error("Wrong number of elements red", n)
	}
	if buf[0] != confs[0] {
		t.Error("Wrong element seeked")
	}
	sn, err = tape.Seek(2, io.SeekCurrent)
	if sn != 2*len(confs)-1 {
		t.Error("Seek problem", sn)
	}
	n, err = tape.Read(buf)
	if err != nil {
		t.Error(err)
	}
	if n != 1 {
		t.Error("Wrong number of elements red", n)
	}
	if buf[0] != confs[3] {
		t.Error("Wrong element seeked")
	}
	sn, err = tape.Seek(0, io.SeekEnd)
	if sn != 2*len(confs)-1 {
		t.Error("Seek problem", sn)
	}
	n, err = tape.Read(buf)
	if err != nil {
		t.Error(err)
	}
	if n != 1 {
		t.Error("Wrong number of elements red", n)
	}
	if buf[0] != confs[3] {
		t.Error("Wrong element seeked")
	}
	sn, err = tape.Seek(2, io.SeekEnd)
	if sn != 2*len(confs)-3 {
		t.Error("Seek problem", sn)
	}
	n, err = tape.Read(buf)
	if err != nil {
		t.Error(err)
	}
	if n != 1 {
		t.Error("Wrong number of elements red", n)
	}
	if buf[0] != confs[1] {
		t.Error("Wrong element seeked")
	}
	sn, err = tape.Seek(1, io.SeekCurrent)
	if sn != 2*len(confs)-1 {
		t.Error("Seek problem", sn)
	}
	n, err = tape.Read(buf)
	if err != nil {
		t.Error(err)
	}
	if n != 1 {
		t.Error("Wrong number of elements red", n)
	}
	if buf[0] != confs[3] {
		t.Error("Wrong element seeked")
	}

	sn, err = tape.Seek(1, io.SeekCurrent)
	if sn != -1 {
		t.Error("Seek problem", sn)
	}

	sn, err = tape.Seek(10, io.SeekCurrent)
	if sn != -1 {
		t.Error("Seek problem", sn)
	}

	sn, err = tape.Seek(-1, io.SeekEnd)
	if sn != -1 {
		t.Error("Seek problem", sn)
	}

	sn, err = tape.Seek(-1, io.SeekStart)
	if sn != -1 {
		t.Error("Seek problem", sn)
	}

}

func TestFilter(t *testing.T) {
	tape = NewRAMTape[gts.Conf](gts.NewConf("RAMTape"))
	for i, v := range confs {
		v.Set(gts.NewPair("a", i+1))
		tape.Append(v)
	}
	tape.Append(confs...)

	newTape := NewRAMTape[gts.Conf](gts.NewConf("DestinationRAMTTape"))

	tape.Filter(newTape, func(e gts.Conf) bool { return e.Get("a") == 1 })

	buf := make([]gts.Conf, 10)
	n, err := newTape.Read(buf)
	if err != nil {
		t.Error(err)
	}
	if n != 2 {
		t.Error("Unexpected count of filtered values")
	}

	tape.Filter(newTape, func(e gts.Conf) bool { return e.Get("a") == 10 })

	buf = make([]gts.Conf, 10)
	n, err = newTape.Read(buf)
	if err != nil {
		t.Error(err)
	}
	if n != 0 {
		t.Error("Unexpected count of filtered values")
	}
}
