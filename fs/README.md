# Gonatus File System
Gonatus file system redefines the the architecture of a file system. The term "directory" does not exist, all records in the FS are called files. Each file may have content, topology (child records) or both (so files can also behave as directories).

## File types
  * `undetermined` - The file does not exist or its state is unknown.
  * `topology` - The file can have descendants (directory in traditional FS).
  * `content` - The file holds a binary content (file in traditional FS).
  * `topology|content` - The file has both topology and content (file and directory at the same time, impossible in traditional FS).

## Limitations
Design of the filesystem moves limitation to the edge of the technical possibilities of compute machines generally.
  * **Path length -** limited by RAM size.
  * **Path forbidden characters -** none (also `/` and `\` may be used).
  * **Records count -** according to the used storage driver (usually near to mmap limits).
  * **Record content size -** limited by the storage capacity.
  * **Record topology depth -** unlimited (listing is done via the Lazy Streams).

## Path
File path is internally defined as a type derived  a slice of strings. Each slice element is meant to be a name of a path level. For example `/home/foo/bar/c.bin` is represented as `["home", "foo", "bar", "c.bin"]`. The advantages are that all slice operations may be used and there are no forbidden characters.

## Usage

### Storage
Storage is a file system device abstraction. Every storage has its own storage driver, which is a set of methods for communication with the data storage device. The storage interface encapsulates the actual device used and allowes to use all of them the same way.

The public API of the storage:
```go
type Storage interface {
	gonatus.Gobjecter

	Merge(source Storage) error
	Tree(depth Depth) (streams.ReadableOutputStreamer[File], error)

	Commit() error
	Clear() error

	Id() StorageId
}
```

There are two reference storage driver implementations in the `drivers` package. Native storage is a read-only access point to the file system on the local machine. Local counted storage stores data as binary files in a tree of counted folders. Physical location of the files is independent of the virtual file table. That avoids all naming and path restrictions. You can create your own driver by implementing the `StorageDriver` interface:
```go
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

	Features() StorageFeatures
	Id() StorageId
	SetId(id StorageId)
}
```

To use it, you have to register the storage to the storage manager. The storage is a global Gobject managing all storages used in the system and assigning each of them its unique ID.

### File
File is a basic interface of the file system. It gives access to all operations needed for the FS to use. The file Gobject is created by a standard constructor `NewFile(FileConf) File`. When created, the file is not initially attached to the storage with the given ID (it even does not have to exist), the corresponding storage driver is accessed when some operation is performed with the file.

If file flags are specified in the configuration structure, they are used if `Open()` or `MkDir()` methods are invoked. For example, if you open a file with the topology flag set, it will be stored with both content and topology flags. User is also able to manually set an original creation time (if not specified, current time is used).

The configuration structure:
```go
type FileConf struct {
  Path      Path
  StorageId StorageId
  Flags     FileFlags
  OrigTime  time.Time
}
```

The file interface:
```go
type File interface {
	gonatus.Gobjecter
	io.Closer
	FileDescriptor

	Storage() Storage

	Path() Path
	Name() string

	Copy(dst File) error
	Move(dst File) error
	Delete() error

	Open(mode FileMode) error
	MkDir() error

	Tree(depth Depth) (streams.ReadableOutputStreamer[File], error)
	Stat() (FileStat, error)

	SetOrigTime(time time.Time)
}
```

### File descriptor
The `FileDescriptor` interface mimics the standard Golang file descriptor, but abstracts a handler of an opened file in any storage. It is returned by by `Open()` method of the storage driver and is integrated in the `File` interface. However, the descriptor methods can only be used if the file is open.

## Example

```go
nativeStorage := drivers.NewNativeStorage(drivers.NativeStorageConf{
	Prefix: "/home/admin/Documents",
})
fs.GStorageManager.RegisterStorage(nativeStorage)

targetStorage := drivers.NewLocalCountedStorage(drivers.LocalCountedStorageConf{
	Prefix: "/tmp/storage",
})
fs.GStorageManager.RegisterStorage(targetStorage)

sourceFile := fs.NewFile(fs.FileConf{
	StorageId: nativeStorage.Id(),
	Path:      fs.Path{"trash", "fs.go"},
})

destinationFile := fs.NewFile(fs.FileConf{
	StorageId: targetStorage.Id(),
	Path:      fs.Path{"c", "copy.txt"},
})

if err := sourceFile.Copy(destinationFile); err != nil {
	panic(err)
}
``` 
