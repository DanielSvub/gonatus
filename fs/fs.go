package fs

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
	"github.com/SpongeData-cz/gonatus/streams"
)

type FileMode uint8

const (
	ModeRead = iota
	ModeWrite
	ModeAppend
	ModeRW
)

type Path []string

func (ego Path) Join(apendee Path) Path {
	return append(ego, apendee...)
}

func (ego Path) Clone() Path {
	return append(make(Path, 0), ego...)
}

func (ego Path) Equals(another Path) bool {
	if len(ego) != len(another) {
		return false
	}
	for i := range ego {
		if ego[i] != another[i] {
			return false
		}
	}
	return true
}

func (ego Path) Dir() Path {
	length := len(ego)
	if length <= 1 {
		return Path{}
	}
	return ego[:length-1]
}

func (ego Path) Base() string {
	length := len(ego)
	if length == 0 {
		return "/"
	}
	return ego[length-1]
}

func (ego Path) String() string {
	return "/" + strings.Join(ego, "/")
}

type StorageId uint64

type StorageManager struct {
	counter            StorageId
	registeredStorages map[StorageId]Storage
}

var GStorageManager StorageManager = StorageManager{registeredStorages: make(map[StorageId]Storage)}

func (ego *StorageManager) RegisterStorage(s Storage) {
	ego.counter++
	ego.registeredStorages[ego.counter] = s
	s.driver().SetId(ego.counter)
}

func (ego *StorageManager) UnregisterStorage(s Storage) error {
	index, err := ego.GetId(s)
	if err != nil {
		return err
	}
	delete(ego.registeredStorages, index)
	s.driver().SetId(0)
	return nil
}

func (ego *StorageManager) Fetch(e StorageId) (Storage, error) {
	if ego.registeredStorages[e] == nil {
		return nil, errors.NewNotFoundError(ego, errors.LevelError, "No storage with index "+fmt.Sprint(e)+".")
	}
	return ego.registeredStorages[e], nil
}

func (ego *StorageManager) GetId(s Storage) (StorageId, error) {
	for id, ss := range ego.registeredStorages {
		if s.driver() == ss.driver() {
			return id, nil
		}
	}
	return *new(StorageId), errors.NewNotFoundError(ego, errors.LevelError, "Storage not found.")
}

func (ego *StorageManager) Serialize() gonatus.Conf {
	return nil
}

type FileDescriptor interface {
	io.Reader
	io.ReaderFrom
	io.Writer
	io.Seeker
}

type File interface {
	gonatus.Gobjecter
	FileDescriptor

	Storage() Storage

	Path() Path
	Name() string

	Copy(dst File) error
	Move(dst File) error
	Delete() error

	Open(mode FileMode) error
	io.Closer

	MkDir() error
	Tree(depth Depth) (streams.ReadableOutputStreamer[File], error)

	Stat() (FileStat, error)
	SetOrigTime(time time.Time)
}

type Storage interface {
	gonatus.Gobjecter

	driver() StorageDriver

	Merge(source Storage) error
	Tree(depth Depth) (streams.ReadableOutputStreamer[File], error)

	Commit() error
	Clear() error
}

type StorageDriver interface {
	gonatus.Gobjecter

	Open(path Path, mode FileMode, givenFlags FileFlags, origTime time.Time) (FileDescriptor, error)
	Close(path Path) error

	MkDir(path Path, origTime time.Time) error

	Copy(srcPath Path, dstPath Path) error
	Move(srcPath Path, dstPath Path) error
	Delete(path Path) error

	Tree(path Path, depth Depth) (streams.ReadableOutputStreamer[File], error)

	Size(path Path) (uint64, error)
	Flags(path Path) (FileFlags, error)

	Commit() error
	Clear() error

	Id() StorageId
	SetId(id StorageId)
}
