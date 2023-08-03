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

/*
Mode in which a file could be opened.
*/
type FileMode uint8

const (
	ModeRead = iota
	ModeWrite
	ModeAppend
	ModeRW
)

/*
Features of a storage (flag byte).
*/
type StorageFeatures uint8

const (
	FeatureRead StorageFeatures = 1 << iota
	FeatureWrite
	FeatureLocation
)

/*
Representation of the file path.
*/
type Path []string

/*
Joins two paths into a single one.

Parameters:
  - apendee - path to append.

Returns:
  - concated path.
*/
func (ego Path) Join(apendee Path) Path {
	return append(ego, apendee...)
}

/*
Creates a copy of the path.

Returns:
  - new path.
*/
func (ego Path) Clone() Path {
	return append(make(Path, 0), ego...)
}

/*
Checks if the path is equal to another path.

Parameters:
  - another - path to compare with.

Returns:
  - true if the paths are equal, false otherwise.
*/
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

/*
Acquires the parent path of the path.

Returns:
  - path without the last element.
*/
func (ego Path) Dir() Path {
	length := len(ego)
	if length <= 1 {
		return Path{}
	}
	return ego[:length-1]
}

/*
Acquires the name of the file.

Returns:
  - last element of the path.
*/
func (ego Path) Base() string {
	length := len(ego)
	if length == 0 {
		return "/"
	}
	return ego[length-1]
}

/*
Acquires a string representation of the path.

Returns:
  - serialized path.
*/
func (ego Path) String() string {
	return "/" + strings.Join(ego, "/")
}

/*
Identification of the storage.
*/
type StorageId uint64

/*
A service which keeps track of the storages.

Extends:
  - gonatus.Gobject.

Implements:
  - gonatus.Gobjecter.
*/
type StorageManager struct {
	gonatus.Gobject
	counter            StorageId
	registeredStorages map[StorageId]Storage
}

// Default storage manager
var GStorageManager StorageManager = StorageManager{registeredStorages: make(map[StorageId]Storage)}

/*
Registers a new storage to the manager.

Parameters:
  - s - storage to register.
*/
func (ego *StorageManager) RegisterStorage(s Storage) {
	ego.counter++
	ego.registeredStorages[ego.counter] = s
	s.driver().SetId(ego.counter)
}

/*
Unregisters a storage from the manager.

Parameters:
  - s - storage to unregister.

Returns:
  - error if any occurred.
*/
func (ego *StorageManager) UnregisterStorage(s Storage) error {
	index, err := ego.GetId(s)
	if err != nil {
		return err
	}
	delete(ego.registeredStorages, index)
	s.driver().SetId(0)
	return nil
}

/*
Fetches a storage with the given ID.

Parameters:
  - e - ID of the storage.

Returns:
  - the storage (nil if not found),
  - error if not found.
*/
func (ego *StorageManager) Fetch(e StorageId) (Storage, error) {
	if ego.registeredStorages[e] == nil {
		return nil, errors.NewNotFoundError(ego, errors.LevelError, "No storage with index "+fmt.Sprint(e)+".")
	}
	return ego.registeredStorages[e], nil
}

/*
Acquires an ID of the given storage.

Parameters:
  - s - the storage.

Returns:
  - ID of the storage (0 if not found),
  - error if not found.
*/
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

/*
Gonatus abstraction of the Go file descriptor.

Extends:
  - io.Reader
  - io.ReaderFrom
  - io.Writer
  - io.Seeker
*/
type FileDescriptor interface {
	io.Reader
	io.ReaderFrom
	io.ReaderAt
	io.Writer
	io.Seeker
}

/*
A file in the Gonatus File System.

Extends:
  - gonatus.Gobjecter,
  - io.Closer,
  - FileDescriptor.
*/
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
		Acquires the real location (native path) of the file.

		Returns:
		  - native path,
		  - error if any occurred.
	*/
	Location() (string, error)

	/*
		Acquires the name of the file (last element of the path).

		Returns:
		  - the name.
	*/
	Name() string

	/*
		Copies content and topology of the file to another one.
		If the files are located on the same storage, invokes same-storage copy method.

		Parameters:
		  - dst - destination file.

		Returns:
		  - error if any occurred.
	*/
	Copy(dst File) error

	/*
		Moves content and topology of the file to another one.
		If the files are located on the same storage, invokes same-storage move method, otherwise copies the file and then deletes it.

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

/*
Abstraction of the Gonatus storage.
The actual implementation depends on the storage driver.

Extends:
  - gonatus.Gobjecter.
*/
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

/*
Storage driver. Implements the behavior of the storage.

Extends:
  - gonatus.Gobjecter.
*/
type StorageDriver interface {
	gonatus.Gobjecter

	/*
		Opens a file.

		Parameters:
		  - path - path to the file,
		  - mode - opening mode,
		  - givenFlags - flags to open with (content is implicit),
		  - origTime - time when the file was originally created.

		Returns:
		  - file descriptor,
		  - error if any occurred.
	*/
	Open(path Path, mode FileMode, givenFlags FileFlags, origTime time.Time) (FileDescriptor, error)

	/*
		Closes a file.

		Parameters:
		  - path - path to the file.

		Returns:
		  - error if any occurred.
	*/
	Close(path Path) error

	/*
		Adds the topology flag to the file.
		If the file does not exist, it is created.

		Parameters:
		  - path - path to the file,
		  - origTime - time when the file was originally created.

		Returns:
		  - error if any occurred.
	*/
	MkDir(path Path, origTime time.Time) error

	/*
		Performs an same-storage copy.

		Parameters:
		  - srcPath - path to the source file,
		  - dstPath - path to the destination file.

		Returns:
		  - error if any occurred.
	*/
	Copy(srcPath Path, dstPath Path) error

	/*
		Performs an same-storage move.

		Parameters:
		  - srcPath - path to the source file,
		  - dstPath - path to the destination file.

		Returns:
		  - error if any occurred.
	*/
	Move(srcPath Path, dstPath Path) error

	/*
		Deletes a file.

		Parameters:
		  - path - path to the file.

		Returns:
		  - error if any occurred.
	*/
	Delete(path Path) error

	/*
		Performs a tree request over the storage.

		Parameters:
		  - path - path where to start,
		  - depth - how deep to go in the file tree.

		Returns:
		  - stream of the files,
		  - error if any occurred.
	*/
	Tree(path Path, depth Depth) (streams.ReadableOutputStreamer[File], error)

	/*
		Acquires a size of a file with the given path.

		Parameters:
		  - path - path to the file.

		Returns:
		  - size of the file,
		  - error if any occurred.
	*/
	Size(path Path) (uint64, error)

	/*
		Acquires flags of a file with the given path.

		Parameters:
		  - path - path to the file.

		Returns:
		  - flags of the file,
		  - error if any occurred.
	*/
	Flags(path Path) (FileFlags, error)

	/*
		Acquires real location (native path) of a file with the given path.

		Parameters:
		  - path - path to the file.

		Returns:
		  - native path,
		  - error if any occurred.
	*/
	Location(path Path) (string, error)

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
		Acquires the feature flag byte of the storage.

		Returns:
		  - feature flags.
	*/
	Features() StorageFeatures

	/*
		Acquires the ID of the storage.
		If the storage is not registered, returns 0.

		Returns:
		  - ID of the storage.
	*/
	Id() StorageId

	/*
		Sets the storage ID.

		Parameters:
		  - ID to set.
	*/
	SetId(id StorageId)
}
