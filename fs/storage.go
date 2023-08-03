package fs

import (
	"math"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/streams"
)

/*
Depth of the tree command.
*/
type Depth uint64

const (
	DepthUnlimited Depth = math.MaxUint64
	DepthSelf      Depth = iota - 1
	DepthLs
)

/*
Gonatus storage structure.

Extends:
  - gonatus.Gobject.

Implements:
  - Storage.
*/
type storage struct {
	gonatus.Gobject
	drv StorageDriver
}

/*
Storage constructor.
Meant to be used only from inside of storage drivers.

Parameters:
  - driver - driver for the storage.

Returns:
  - pointer to the created storage.
*/
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
			Path:      ego.driver().AbsPath(srcFile.Path()),
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

func (ego *storage) ChDir(path Path) error {
	return ego.drv.SetCwd(path)
}

func (ego *storage) Commit() error {
	return ego.drv.Commit()
}

func (ego *storage) Clear() error {
	return ego.drv.Clear()
}

func (ego *storage) Id() StorageId {
	return ego.drv.Id()
}

func (ego *storage) Serialize() gonatus.Conf {
	return ego.drv.Serialize()
}
