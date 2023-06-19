package driver

import (
	"fmt"
	"io"
	"os"
	pathlib "path"
	"time"

	"github.com/SpongeData-cz/gonatus/adt"
	"github.com/SpongeData-cz/gonatus/errors"
	"github.com/SpongeData-cz/gonatus/fs"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/streams"
)

type localFileDescriptor struct {
	fd *os.File
}

func (ego *localFileDescriptor) Read(p []byte) (n int, err error) {
	return ego.fd.Read(p)
}

func (ego *localFileDescriptor) ReadFrom(r io.Reader) (n int64, err error) {
	return ego.fd.ReadFrom(r)
}

func (ego *localFileDescriptor) Write(p []byte) (n int, err error) {
	return ego.fd.Write(p)
}

func (ego *localFileDescriptor) Seek(offset int64, whence int) (int64, error) {
	return ego.fd.Seek(offset, whence)
}

type contentEntry struct {
	id       int
	location string
}

type fileEntry struct {
	id        int
	path      adt.List[string]
	parent    int
	flags     fs.FileFlags
	origTime  time.Time
	modifTime time.Time
}

type LocalStorageConf struct {
	Prefix string
}

type localStorageDriver struct {
	gonatus.Gobject
	id            fs.StorageId
	prefix        string
	files         adt.List[*fileEntry]
	contents      adt.List[*contentEntry]
	openFiles     adt.Dict[int, *os.File]
	locationCount uint64
	dirCount      int
}

func NewLocalStorage(conf LocalStorageConf) fs.Storage {
	now := time.Now()
	driver := new(localStorageDriver)
	driver.prefix = conf.Prefix
	driver.contents = adt.NewList[*contentEntry]()
	driver.files = adt.NewList[*fileEntry]()
	driver.files.Add(&fileEntry{
		id:        0,
		path:      adt.NewList[string](),
		parent:    -1,
		flags:     fs.FileTopology,
		origTime:  now,
		modifTime: now,
	})
	driver.openFiles = adt.NewDict[int, *os.File]()
	return fs.NewStorage(driver)
}

/*
Creates a new counted storage location.
Increments the file counter and creates the directory tree in local file system, if it does not exist.

Returns:
  - location - destination fullpath (12 numbers splitted by 3 + ".bin"),
  - err - error if any occurred.
*/
func (ego *localStorageDriver) newLocation() (location string, err error) {
	str := fmt.Sprintf("%012d", ego.locationCount)
	ego.locationCount++
	location = pathlib.Join(ego.prefix, str[:3]+"/"+str[3:6]+"/"+str[6:9])
	err = os.MkdirAll(location, os.ModePerm)
	location += "/" + str[9:] + ".bin"
	return
}

/*
Recursively creates ancestor directories for a file.

Parameters:
  - path - a path to create,
  - origTime - time when the file was originally created.

Returns:
  - error if any occurred.
*/
func (ego *localStorageDriver) createPath(path adt.List[string], origTime time.Time) (err error) {
	id := ego.findFile(path)
	if id >= 0 {
		return
	}
	err = ego.MkDir(fs.Path(path.GoSlice()), origTime)
	return
}

/*
Crates an entries in the file table and content table.

Parameters:
  - path - path to the file,
  - location - a physical location of the file on the disk,
  - givenFlags - flags entered in FileConf,
  - origTime - time when the file was originally created.

Returns:
  - id - ID of the created file,
  - err - error if any occurred.
*/
func (ego *localStorageDriver) createFile(path fs.Path, location string, givenFlags fs.FileFlags, origTime time.Time) (id int, err error) {
	parent := ego.findFile(adt.NewListFrom(path.Dir()))
	ego.dirCount++
	id = ego.dirCount
	ego.files.Add(&fileEntry{
		id:        id,
		parent:    parent,
		path:      adt.NewListFrom(path).Clone(),
		flags:     givenFlags | fs.FileContent,
		origTime:  origTime,
		modifTime: time.Now(),
	})
	ego.addContent(id, location)
	return
}

/*
Crates an entry in the file table without a content entry.

Parameters:
  - path - path to the file,
  - origTime - time when the file was originally created.

Returns:
  - error if any occurred.
*/
func (ego *localStorageDriver) createDir(path fs.Path, origTime time.Time) error {
	parent := ego.findFile(adt.NewListFrom(path.Dir()))
	ego.dirCount++
	now := time.Now()
	ego.files.Add(&fileEntry{
		id:        ego.dirCount,
		parent:    parent,
		path:      adt.NewListFrom(path).Clone(),
		flags:     fs.FileTopology,
		origTime:  origTime,
		modifTime: now,
	})
	return nil
}

/*
Recursively deletes a file with the given ID and all its descendants.

Parameters:
  - id - ID of the file.
*/
func (ego *localStorageDriver) deleteDirectory(id int) {
	for i, dir := range ego.files.GoSlice() {
		if dir.id == id {
			ego.files.Delete(i)
			ego.filesWithParent(id).ForEach(func(child *fileEntry) {
				for j, file := range ego.contents.GoSlice() {
					if file.id == child.id {
						ego.contents.Delete(j)
						break
					}
				}
				ego.deleteDirectory(child.id)
			})
			break
		}
	}
}

/*
Deletes a file.

Parameters:
  - path - path to the file.

Returns:
  - error if any occurred.
*/
func (ego *localStorageDriver) delete(path adt.List[string]) error {
	if exists, flags, id, location := ego.search(path); exists {
		ego.deleteDirectory(id)
		if flags&fs.FileContent > 0 {
			for j, file := range ego.contents.GoSlice() {
				if file.id == id {
					ego.contents.Delete(j)
					break
				}
			}
			for _, file := range ego.contents.GoSlice() {
				if file.location == location {
					return nil
				}
			}
			if err := os.Remove(location); err != nil {
				return err
			}
		}
		return nil
	}
	return errors.NewNotFoundError(ego, errors.LevelError, path.Pop())
}

/*
Acquires a list of all files with the given parent.

Parameters:
  - parent - ID of the parent file.

Returns:
  - list of the files.
*/
func (ego *localStorageDriver) filesWithParent(parent int) adt.List[*fileEntry] {
	return ego.files.Filter(func(dir *fileEntry) bool { return dir.parent == parent })
}

func (ego *localStorageDriver) getFile(entry *fileEntry) fs.File {
	var flags fs.FileFlags
	if ego.contents.Search(func(f *contentEntry) bool { return f.id == entry.id }) != nil {
		flags |= fs.FileContent
	}
	return fs.NewFile(fs.FileConf{
		Path:      entry.path.Clone().GoSlice(),
		StorageId: ego.id,
		Flags:     flags,
		OrigTime:  entry.origTime,
	})
}

/*
Creates a stream of the topology of the file.

Parameters:
  - path - path to the file.

Returns:
  - readable output stream of files,
  - error if any occurred.
*/
func (ego *localStorageDriver) fileTopology(path adt.List[string]) (streams.ReadableOutputStreamer[fs.File], error) {
	id := ego.findFile(path)
	if id < 0 {
		return nil, errors.NewNotFoundError(ego, errors.LevelError, "The path does not exist.")
	}
	dirs := ego.filesWithParent(id)

	inputStream := streams.NewBufferInputStream[fs.File](1) // TODO buffersize
	outputStream := streams.NewReadableOutputStream[fs.File]()
	inputStream.Pipe(outputStream)

	exportDir := func(stream streams.BufferInputStreamer[fs.File]) {
		dirs.ForEach(func(entry *fileEntry) {
			dir := ego.getFile(entry)
			stream.Write(dir)
		})
		stream.Close()
	}

	go exportDir(inputStream)

	return outputStream, nil
}

/*
Acquires an ID of the file on the given path.

Parameters:
  - path - path to the file.

Returns:
  - ID of the found file, -1 if the path does not exist.
*/
func (ego *localStorageDriver) findFile(path adt.List[string]) int {

	if path.Empty() {
		return 0
	}

	for _, file := range ego.files.GoSlice() {
		if file.path.Equals(path) {
			return file.id
		}
	}

	return -1

}

/*
Searches for a file.

Parameters:
  - path - path to the file.

Returns:
  - exists - whether the file exists,
  - flags - flags of the file,
  - id - file ID,
  - location - a physical location of the file on the disk (empty string for files without content).
*/
func (ego *localStorageDriver) search(path adt.List[string]) (exists bool, flags fs.FileFlags, id int, location string) {

	for _, file := range ego.files.GoSlice() {
		if file.path.Equals(path) {
			exists = true
			id = file.id
			flags = file.flags
			if flags&fs.FileContent > 0 {
				content := ego.contents.Search(func(content *contentEntry) bool { return content.id == file.id })
				location = (*content).location
				for _, content := range ego.contents.GoSlice() {
					if content.id == file.id {
						location = content.location
						return
					}
				}
			}
			break
		}
	}

	return

}

/*
Splits a path to last file and path to its parent.

Parameters:
  - path - a path to split.

Returns:
  - dirPath - path to the parent file,
  - dirName - name of the parent file.
*/
func (ego *localStorageDriver) splitPath(path adt.List[string]) (dirPath adt.List[string], dirName string) {
	if path.Empty() {
		dirPath = adt.NewList[string]()
		return
	}
	dirPath = path.Clone()
	dirName = dirPath.Pop()
	return
}

/*
Sets a new parent to a file.

Parameters:
  - source - path to the file,
  - dest - path where the file should be moved.

Returns:
  - error if any occurred.
*/
func (ego *localStorageDriver) moveFile(source adt.List[string], dest adt.List[string]) error {
	if exists, _, _, _ := ego.search(dest); exists {
		return errors.NewStateError(ego, errors.LevelError, "File of the same name already exists in the destination path.")
	}
	id := ego.findFile(source)
	if id < 0 {
		return errors.NewNotFoundError(ego, errors.LevelError, `The file "`+source.Pop()+`" does not exist.`)
	}
	newParent := ego.findFile(dest.Clone().Delete(dest.Count() - 1))
	if newParent < 0 {
		return errors.NewNotFoundError(ego, errors.LevelError, "The destination path does not exist.")
	}
	for _, file := range ego.files.GoSlice() {
		if file.id == id {
			file.parent = newParent
			file.path = dest
			file.flags = (*ego.files.Search(func(x *fileEntry) bool { return x.id == id })).flags
			file.modifTime = time.Now()
			return nil
		}
	}
	return errors.NewUnknownError(ego)
}

/*
Creates a recursive copy of a file.

Parameters:
  - source - path to the file,
  - parent - ID of the parent file,
  - dest - path where the file should be moved.

Returns:
  - error if any occurred.
*/
func (ego *localStorageDriver) copyFile(source adt.List[string], parent int, dest adt.List[string]) (err error) {

	exists, flags, id, location := ego.search(source)

	if !exists {
		return errors.NewNotFoundError(ego, errors.LevelError, `The file "`+source.Pop()+`" does not exist.`)
	}

	if exists, _, _, _ := ego.search(dest); exists {
		return errors.NewStateError(ego, errors.LevelError, "File of the same name already exists in the destination path.")
	}

	origTime := (*ego.files.Search(func(x *fileEntry) bool { return x.id == id })).origTime

	srcFd, err := ego.Open(source.GoSlice(), fs.ModeRead, flags, origTime)
	if err != nil {
		return err
	}
	defer ego.Close(source.GoSlice())

	newLocation, err := ego.newLocation()
	if err != nil {
		return err
	}

	dstFd, err := os.OpenFile(newLocation, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}

	defer dstFd.Close()

	dstFd.ReadFrom(srcFd)

	create := func(file bool, path adt.List[string], parent int, id int, location string) error {
		if file {
			ego.contents.Add(&contentEntry{
				id:       id,
				location: newLocation,
			})
			flags |= fs.FileContent
		}
		ego.files.Add(&fileEntry{
			id:        id,
			parent:    parent,
			path:      path.Clone(),
			flags:     flags,
			origTime:  origTime,
			modifTime: time.Now(),
		})
		return nil
	}

	ego.dirCount++
	newId := ego.dirCount
	ego.filesWithParent(id).ForEach(func(entry *fileEntry) {
		err = ego.copyFile(entry.path, newId, dest)
	})
	dirPath, _ := ego.splitPath(dest)
	if err := ego.MkDir(dirPath.GoSlice(), origTime); err != nil {
		return err
	}

	return create(flags&fs.FileContent > 0, dest, parent, newId, location)

}

/*
Exports files to a stream.

Parameters:
  - path - where to begin the export,
  - depth - how many levels under the file specified with path to export (0 for unlimited depth, 1 for LS).

Returns:
  - the readable stream with result,
  - error if any occurred.
*/
func (ego *localStorageDriver) exportToStream(path adt.List[string], depth fs.Depth) (streams.ReadableOutputStreamer[fs.File], error) {

	if ego.findFile(path) < 0 {
		return nil, errors.NewNotFoundError(ego, errors.LevelError, "The path does not exist.")
	}

	files := ego.files.Filter(func(entry *fileEntry) bool {
		for i, dir := range path.GoSlice() {
			if entry.path.Count() <= i || dir != entry.path.Get(i) {
				return false
			}
		}
		if fs.Depth(entry.path.Count()-path.Count()) > depth {
			return false
		}
		return true
	})

	export := func(stream streams.BufferInputStreamer[fs.File]) {
		files.ForEach(func(entry *fileEntry) {
			dir := ego.getFile(entry)
			file, ok := dir.(fs.File)
			if ok {
				stream.Write(file)
			}
		})
		stream.Close()
	}

	inputStream := streams.NewBufferInputStream[fs.File](1) // TODO buffersize
	outputStream := streams.NewReadableOutputStream[fs.File]()
	inputStream.Pipe(outputStream)

	go export(inputStream)

	return outputStream, nil

}

/*
Closes a file.

Parameters:
  - path - path to the file.

Returns:
  - error if any occurred.
*/
func (ego *localStorageDriver) closeFile(path fs.Path) error {
	if _, flags, id, _ := ego.search(adt.NewListFrom(path)); flags&fs.FileContent > 0 {
		if !ego.openFiles.KeyExists(id) {
			return errors.NewStateError(ego, errors.LevelError, `The file "`+path.String()+`" is not open.`)
		}
		ego.openFiles.Get(id).Close()
		ego.openFiles.Unset(id)
		file := ego.files.Search(func(x *fileEntry) bool { return x.id == id })
		(*file).modifTime = time.Now()
		return nil
	}
	return errors.NewNotFoundError(ego, errors.LevelError, `The file "`+path.String()+`" does not exist.`)
}

/*
Adds a content entry for a specified file.

Parameters:
  - id - ID of the file,
  - location - physical location of the content.
*/
func (ego *localStorageDriver) addContent(id int, location string) {
	ego.contents.Add(&contentEntry{
		id:       id,
		location: location,
	})
}

/*
Acquires flags for a file.

Parameters:
  - path - path to the file.

Returns:
  - flags for the file.
*/
func (ego *localStorageDriver) getFlags(path adt.List[string]) fs.FileFlags {
	for _, entry := range ego.files.GoSlice() {
		if entry.path.Equals(path) {
			return entry.flags
		}
	}
	return fs.FileUndetermined
}

func (ego *localStorageDriver) Open(path fs.Path, mode fs.FileMode, givenFlags fs.FileFlags, origTime time.Time) (fs.FileDescriptor, error) {

	// Creating modeFlags
	var modeFlags int
	switch mode {
	case fs.ModeRead:
		modeFlags = os.O_RDONLY
	case fs.ModeWrite:
		modeFlags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	case fs.ModeAppend:
		modeFlags = os.O_WRONLY | os.O_CREATE | os.O_APPEND
	case fs.ModeRW:
		modeFlags = os.O_RDWR | os.O_CREATE
	default:
		return nil, errors.NewMisappError(ego, "Invalid opening mode.")
	}

	var fd *os.File
	var fid int

	// Checking if the file exists
	if exists, flags, id, location := ego.search(adt.NewListFrom(path)); exists {

		var err error

		// If the file does not have content yet, addding it
		if flags&fs.FileContent == fs.FileUndetermined {
			location, err = ego.newLocation()
			if err != nil {
				return nil, err
			}
			ego.addContent(id, location)
		}

		// Opening the existing file
		fd, err = os.OpenFile(location, modeFlags, 0664)
		if err != nil {
			return nil, err
		}

		fid = id

	} else {

		// If in read mode, error (the file cannot be created without write permission)
		if mode == fs.ModeRead {
			return nil, errors.NewNotFoundError(ego, errors.LevelError, `The file "`+path.String()+`" does not exist.`)
		}

		// Checking if the parent file exists, if not, creating it
		if len(path) > 0 {
			dirPath, _ := ego.splitPath(adt.NewListFrom(path))
			if err := ego.MkDir(dirPath.GoSlice(), origTime); err != nil {
				return nil, err
			}
		}

		// Creating a new file and opening it
		var err error
		fullpath, err := ego.newLocation()
		fd, err = os.OpenFile(fullpath, modeFlags, 0664)
		if err != nil {
			return nil, err
		}

		// Creating a file entry
		fid, err = ego.createFile(path, fullpath, givenFlags, origTime)
		if err != nil {
			return nil, err
		}

	}

	ego.openFiles.Set(fid, fd)

	return &localFileDescriptor{
		fd: fd,
	}, nil

}

func (ego *localStorageDriver) Close(path fs.Path) error {
	return ego.closeFile(path)
}

func (ego *localStorageDriver) MkDir(path fs.Path, origTime time.Time) error {
	file := ego.files.Search(func(x *fileEntry) bool {
		return x.path.Equals(adt.NewListFrom(path))
	})
	if file != nil {
		(*file).flags |= fs.FileTopology
		return nil
	}
	parentPath, _ := ego.splitPath(adt.NewListFrom(path).Clone())
	if err := ego.createPath(parentPath, origTime); err != nil {
		return err
	}
	if err := ego.createDir(path, origTime); err != nil {
		return err
	}
	return nil
}

func (ego *localStorageDriver) Copy(srcPath fs.Path, dstPath fs.Path) error {
	if err := ego.copyFile(adt.NewListFrom(srcPath), ego.findFile(adt.NewListFrom(dstPath)), adt.NewListFrom(dstPath)); err != nil {
		return err
	}
	return nil
}

func (ego *localStorageDriver) Move(srcPath fs.Path, dstPath fs.Path) error {
	src := adt.NewListFrom(srcPath)
	dst := adt.NewListFrom(dstPath)
	if !src.Equals(dst) {
		if err := ego.moveFile(src, dst); err != nil {
			return err
		}
	}
	return nil
}

func (ego *localStorageDriver) Delete(path fs.Path) error {
	return ego.delete(adt.NewListFrom(path))
}

func (ego *localStorageDriver) Tree(path fs.Path, depth fs.Depth) (streams.ReadableOutputStreamer[fs.File], error) {
	return ego.exportToStream(adt.NewListFrom(path), depth)
}

func (ego *localStorageDriver) Size(path fs.Path) (uint64, error) {
	var fd *os.File
	id := ego.findFile(adt.NewListFrom(path))
	if !ego.openFiles.KeyExists(id) {
		descriptor, err := ego.Open(path, fs.ModeRead, fs.FileUndetermined, *new(time.Time))
		if err != nil {
			return 0, err
		}
		fd = descriptor.(*localFileDescriptor).fd
		defer ego.Close(path)
	} else {
		fd = ego.openFiles.Get(id)
	}
	stat, err := fd.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(stat.Size()), nil
}

func (ego *localStorageDriver) Flags(path fs.Path) (flags fs.FileFlags, err error) {
	exists, flags, _, _ := ego.search(adt.NewListFrom(path))
	if !exists {
		err = errors.NewNotFoundError(ego, errors.LevelError, `The file "`+path.String()+`" does not exist.`)
	}
	return
}

func (ego *localStorageDriver) Commit() error {
	return nil
}

func (ego *localStorageDriver) Clear() (err error) {
	ego.files.Clear()
	ego.contents.ForEach(func(file *contentEntry) {
		err = os.RemoveAll(ego.prefix)
	})
	ego.contents.Clear()
	ego.dirCount = 0
	ego.locationCount = 0
	return
}

func (ego *localStorageDriver) PrintFAT() {

	println("FILES:")
	println("------")
	ego.files.ForEach(func(entry *fileEntry) {
		println(entry.id, entry.parent, entry.path.String(), entry.flags)
	})

	println()
	println("CONTENTS:")
	println("---------")
	ego.contents.ForEach(func(entry *contentEntry) {
		println(entry.id, entry.location)
	})

}

func (ego *localStorageDriver) Features() fs.StorageFeatures {
	return fs.FeatureRead | fs.FeatureWrite
}

func (ego *localStorageDriver) Id() fs.StorageId {
	return ego.id
}

func (ego *localStorageDriver) SetId(id fs.StorageId) {
	ego.id = id
}

func (ego *localStorageDriver) Serialize() gonatus.Conf {
	return LocalStorageConf{Prefix: ego.prefix}
}
