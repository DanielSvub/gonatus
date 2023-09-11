package fs

import (
	"math"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/stream"
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
	return &storageController{&storage{drv: driver}, Path{}}
}

func (ego *storage) driver() StorageDriver {
	return ego.drv
}

func (ego *storage) merge(source Storage, prefix Path) error {

	s, err := source.Tree(DepthUnlimited)
	if err != nil {
		return err
	}

	return s.ForEach(func(srcFile File) error {

		if stat, err := srcFile.Stat(); err != nil {
			return err
		} else if stat.Flags&FileContent == 0 {
			if err := ego.drv.MkDir(srcFile.Path(), stat.OrigTime); err != nil {
				return err
			}
			return nil
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
			Path:      prefix.Join(srcFile.Path()),
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

		return ego.drv.Close(dstFile.Path())

	})

}

func (ego *storage) Tree(depth Depth) (stream.Producer[File], error) {
	return ego.drv.Tree(Path{}, depth)
}

func (ego *storage) Commit() error {
	return ego.drv.Commit()
}

func (ego *storage) Clear() error {
	return ego.drv.Clear()
}

func (ego *storage) Id() gonatus.GId {
	return ego.drv.Id()
}

func (ego *storage) Serialize() gonatus.Conf {
	return ego.drv.Serialize()
}

type storageController struct {
	*storage
	cwd Path
}

func (ego *storageController) ChDir(path Path) error {
	ego.cwd = path
	return nil
}

func (ego *storageController) Merge(source Storage) error {
	return ego.storage.merge(source, ego.cwd)
}
