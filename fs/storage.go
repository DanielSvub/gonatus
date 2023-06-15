package fs

import (
	"math"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/streams"
)

type Depth uint64

const (
	DepthUnlimited Depth = math.MaxUint64
	DepthSelf      Depth = iota - 1
	DepthLs
)

type storage struct {
	gonatus.Gobject
	drv StorageDriver
}

func NewStorage(driver StorageDriver) Storage {
	return &storage{drv: driver}
}

func (ego *storage) driver() StorageDriver {
	return ego.drv
}

func (ego *storage) Merge(source Storage) error {

	stream, err := source.driver().Tree(Path{}, DepthUnlimited)
	if err != nil {
		return err
	}
	slice := make([]File, 1)

	for !stream.Closed() {

		if n, err := stream.Read(slice); err != nil {
			return err
		} else if n < 1 {
			continue
		}
		srcFile := slice[0]

		if stat, err := srcFile.Stat(); err != nil {
			return err
		} else if stat.Flags&FileContent == 0 {
			if err := ego.drv.MkDir(srcFile.Path(), stat.OrigTime); err != nil {
				return err
			}
			continue
		}

		err := srcFile.Open(ModeRead)
		if err != nil {
			return err
		}

		stat, err := srcFile.Stat()
		if err != nil {
			return err
		}

		dstFile := NewFile(FileConf{
			StorageId: ego.driver().Id(),
			Path:      srcFile.Path(),
			Flags:     stat.Flags,
		})

		err = dstFile.Open(ModeWrite)
		if err != nil {
			return err
		}

		if _, err := dstFile.ReadFrom(srcFile); err != nil {
			return err
		}

		if err := srcFile.Close(); err != nil {
			return err
		}
		if err := ego.drv.Close(srcFile.Path()); err != nil {
			return err
		}

	}

	return nil

}

func (ego *storage) Tree(depth Depth) (streams.ReadableOutputStreamer[File], error) {
	return ego.drv.Tree(Path{}, depth)
}

func (ego *storage) Commit() error {
	return ego.drv.Commit()
}

func (ego *storage) Clear() error {
	return ego.drv.Clear()
}

func (ego *storage) Serialize() gonatus.Conf {
	return ego.drv.Serialize()
}
