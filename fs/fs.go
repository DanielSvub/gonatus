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

type StorageFeature uint8

const (
	FeatureRead StorageFeature = 1 << iota
	FeatureWrite
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
	gonatus.Gobject
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
	io.Closer
	FileDescriptor

	/*
		Acquires a storage where the file is stored.

		Returns:
		  - the storage.
	*/
	Storage() Storage

	/*
		Acquires the fullpath to the file.

		Returns:
		  - path as a slice of strings.
	*/
	Path() Path

	/*
		Acquires the name of the file (last element of the path).

		Returns:
		  - the name.
	*/
	Name() string

	/*
		Copies content and topology of the file to another one.
		If the files are located on the same storage, invokes inter-storage copy method.

		Parameters:
		  - dst - destination file.

		Returns:
		  - error if any occurred.
	*/
	Copy(dst File) error

	/*
		Moves content and topology of the file to another one.
		If the files are located on the same storage, invokes inter-storage move method, otherwise copies the file and then deletes it.

		Parameters:
		  - dst - destination file.

		Returns:
		  - error if any occurred.
	*/
	Move(dst File) error

	/*
		Deletes the file.

		Returns:
		  - error if any occurred.
	*/
	Delete() error

	/*
		Opens the file if the given mode.

		Parameters:
		  - mode - opening mode.
	*/
	Open(mode FileMode) error

	/*
		Adds the topology flag to the file.
		If the file does not exist, it is created.

		Returns:
		  - error if any occurred.
	*/
	MkDir() error

	/*
		Acquires a tree of files to the given depth starting from the storage root. It is returned as a stream.
		If the depth is 0, the stream contains only the file itself.

		Parameters:
		  - depth - how deep to go in the file tree.

		Returns:
		  - stream of the files,
		  - error if any occurred.
	*/
	Tree(depth Depth) (streams.ReadableOutputStreamer[File], error)

	/*
		Acquires a current status of the file.

		Returns:
		  - stat structure,
		  - error if any occurred.
	*/
	Stat() (FileStat, error)

	/*
		Manually sets the time when the file was originally created.

		Parameters:
		  - time - time to set.
	*/
	SetOrigTime(time time.Time)
}

type Storage interface {
	gonatus.Gobjecter

	/*
		Acquires the storage driver.

		Returns:
		  - storage driver.
	*/
	driver() StorageDriver

	/*
		Copies all files in the given storage into this storage.

		Parameters:
		  - source - storage to merge.

		Returns:
		  - error if any occurred.
	*/
	Merge(source Storage) error

	/*
		Acquires a tree of files to the given depth starting from this file. It is returned as a stream.
		If the depth is 0, the stream contains only the file itself.

		Parameters:
		  - depth - how deep to go in the file tree.

		Returns:
		  - stream of the files,
		  - error if any occurred.
	*/
	Tree(depth Depth) (streams.ReadableOutputStreamer[File], error)

	/*
		Commits the changes.

		Returns:
		  - error if any occurred.
	*/
	Commit() error

	/*
		Deletes all files in the storage.

		Returns:
		  - error if any occurred.
	*/
	Clear() error

	/*
		Acquires the ID of the storage.
		If the storage is not registered, returns 0.

		Returns:
		  - ID of the storage.
	*/
	Id() StorageId
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

	Features() StorageFeature
	Id() StorageId
	SetId(id StorageId)
}
