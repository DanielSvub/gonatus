package driver

import (
	"fmt"
	"io"
	"os"
	pathlib "path"
	"sync"
	"time"

	"github.com/SpongeData-cz/gonatus/collection"
	"github.com/SpongeData-cz/gonatus/errors"
	"github.com/SpongeData-cz/gonatus/fs"
	"github.com/SpongeData-cz/stream"

	"github.com/SpongeData-cz/gonatus"
)

/*
Gonatus abstraction of the Golang file descriptor.
*/
type localFileDescriptor struct {
	fd *os.File
}

func (ego *localFileDescriptor) Read(p []byte) (n int, err error) {
	return ego.fd.Read(p)
}

func (ego *localFileDescriptor) ReadFrom(r io.Reader) (n int64, err error) {
	return ego.fd.ReadFrom(r)
}

func (ego *localFileDescriptor) ReadAt(b []byte, off int64) (n int, err error) {
	return ego.fd.ReadAt(b, off)
}

func (ego *localFileDescriptor) Write(p []byte) (n int, err error) {
	return ego.fd.Write(p)
}

func (ego *localFileDescriptor) Seek(offset int64, whence int) (int64, error) {
	return ego.fd.Seek(offset, whence)
}

const (
	fieldParent = iota
	fieldPath
	fieldFlags
	fieldLocation
	fieldOrigTime
	fieldModifTime
)

/*
Abstraction of the collection record. Simplifies getting of the column values.
*/
type record collection.RecordConf

func (ego *record) conf() collection.RecordConf {
	return collection.RecordConf(*ego)
}

func (ego *record) path() fs.Path {
	return fs.Path(ego.Cols[fieldPath].(collection.FieldConf[[]string]).Value)
}

func (ego *record) flags() fs.FileFlags {
	return fs.FileFlags(ego.Cols[fieldFlags].(collection.FieldConf[uint8]).Value)
}

func (ego *record) location() string {
	return ego.Cols[fieldLocation].(collection.FieldConf[string]).Value
}

func (ego *record) origTime() time.Time {
	return ego.Cols[fieldOrigTime].(collection.FieldConf[time.Time]).Value
}

const rootId collection.CId = 1

type LocalCountedStorageConf struct {
	Prefix string
}

type localCountedStorageDriver struct {
	gonatus.Gobject
	id            gonatus.GId
	prefix        string
	files         collection.Collection
	openFiles     map[collection.CId]*os.File
	fileLocks     map[collection.CId]*sync.Mutex
	globalLock    sync.Mutex
	fileCount     collection.CId
	locationCount uint64
}

func NewLocalCountedStorage(conf LocalCountedStorageConf) fs.Storage {
	ego := new(localCountedStorageDriver)
	ego.prefix = conf.Prefix
	ego.files = collection.NewRamCollection(collection.RamCollectionConf{
		SchemaConf: collection.SchemaConf{
			Name:         "FileTable",
			FieldsNaming: []string{"parent", "path", "flags", "location", "origTime", "modifTime"},
			Fields: []collection.FielderConf{
				collection.FieldConf[uint64]{},
				collection.FieldConf[[]string]{},
				collection.FieldConf[uint8]{},
				collection.FieldConf[string]{},
				collection.FieldConf[time.Time]{},
				collection.FieldConf[time.Time]{},
			},
			Indexes: [][]collection.IndexerConf{{
				collection.PrefixIndexConf[[]string]{Name: "path"},
				collection.FullmatchIndexConf[[]string]{Name: "path"},
				collection.FullmatchIndexConf[uint64]{Name: "parent"},
			}},
		},
	})
	ego.openFiles = make(map[collection.CId]*os.File)
	ego.fileLocks = make(map[collection.CId]*sync.Mutex)
	ego.createRoot()
	return fs.NewStorage(ego)
}

/*
Creates a record for the root of the FS.

Returns:
  - error if any occurred.
*/
func (ego *localCountedStorageDriver) createRoot() error {
	ego.fileCount++
	now := time.Now()
	_, err := ego.files.AddRecord(collection.RecordConf{
		Id: rootId,
		Cols: []collection.FielderConf{
			collection.FieldConf[uint64]{Value: 0},
			collection.FieldConf[[]string]{Value: []string{}},
			collection.FieldConf[uint8]{Value: uint8(fs.FileTopology)},
			collection.FieldConf[string]{},
			collection.FieldConf[time.Time]{Value: now},
			collection.FieldConf[time.Time]{Value: now},
		},
	})
	return err
}

/*
Acquires the record of the file with the given path.

Parameters:
  - absPath - absolute path to the file.

Returns:
  - pointer to the record of the found file, nil if the path does not exist,
  - error if any occurred.
*/
func (ego *localCountedStorageDriver) findFile(absPath fs.Path) (*record, error) {

	if s, err := ego.files.Filter(collection.FilterArgument{
		Limit: collection.NO_LIMIT,
		QueryConf: collection.QueryAtomConf{
			Name:      "path",
			Value:     []string(absPath),
			MatchType: collection.FullmatchIndexConf[[]string]{},
		}}); err != nil {
		return nil, err
	} else {
		if value, valid, err := s.Get(); err != nil || !valid {
			return nil, err
		} else {
			if _, valid, _ := s.Get(); valid {
				return nil, errors.NewStateError(ego, errors.LevelError, "Multiple records found for a single path "+absPath.String()+".")
			}
			rec := record(value)
			return &rec, nil
		}
	}

}

/*
Executes the given function over records of all files in the given path (unlimited recurse).

Parameters:
  - prefix - path prefix to process,
  - fn - function to execute.

Returns:
  - error if any occurred.
*/
func (ego *localCountedStorageDriver) forFilesWithPrefix(prefix fs.Path, fn func(record) error) error {
	if s, err := ego.files.Filter(collection.FilterArgument{
		Limit: collection.NO_LIMIT,
		QueryConf: collection.QueryAtomConf{
			Name:      "path",
			Value:     []string(prefix),
			MatchType: collection.PrefixIndexConf[[]string]{},
		}}); err != nil {
		return err
	} else {
		ts := stream.NewTransformer(func(conf collection.RecordConf) record { return record(conf) })
		s.Pipe(ts)
		return ts.ForEach(fn)
	}
}

/*
Executes the given function over records of all files with the given parent (children of the given file).

Parameters:
  - parent - ID of the parent file,
  - fn - function to execute.

Returns:
  - error if any occurred.
*/
func (ego *localCountedStorageDriver) forFilesWithParent(parent collection.CId, fn func(record) error) error {
	if s, err := ego.files.Filter(collection.FilterArgument{
		Limit: collection.NO_LIMIT,
		QueryConf: collection.QueryAtomConf{
			Name:      "parent",
			Value:     uint64(parent),
			MatchType: collection.FullmatchIndexConf[uint64]{},
		}}); err != nil {
		return err
	} else {
		ts := stream.NewTransformer(func(conf collection.RecordConf) record { return record(conf) })
		s.Pipe(ts)
		return ts.ForEach(fn)
	}
}

/*
Creates a new counted storage location.
Increments the file counter and creates the directory tree in local file system, if it does not exist.

Returns:
  - location - destination fullpath (12 numbers splitted by 3 + ".bin"),
  - err - error if any occurred.
*/
func (ego *localCountedStorageDriver) newLocation() (location string, err error) {
	ego.globalLock.Lock()
	defer ego.globalLock.Unlock()
	str := fmt.Sprintf("%012d", ego.locationCount)
	ego.locationCount++
	location = pathlib.Join(ego.prefix, str[:3], str[3:6], str[6:9])
	err = os.MkdirAll(location, os.ModePerm)
	location = pathlib.Join(location, str[9:]+".bin")
	return
}

/*
Crates an entry in the file table.

Parameters:
  - absPath - absolute path to the file,
  - location - a physical location of the file on the disk (if the file has content, empty otherwise),
  - givenFlags - flags entered in FileConf,
  - origTime - time when the file was originally created.

Returns:
  - id - ID of the created file,
  - err - error if any occurred.
*/
func (ego *localCountedStorageDriver) createFile(absPath fs.Path, location string, givenFlags fs.FileFlags, origTime time.Time) (id collection.CId, err error) {

	parent, err := ego.findFile(absPath.Dir())
	if err != nil {
		return
	}

	if parent == nil {
		err = errors.NewNotFoundError(ego, errors.LevelError, "The path does not exist.")
		return
	}

	ego.globalLock.Lock()
	ego.fileCount++
	id = collection.CId(ego.fileCount)
	ego.fileLocks[id] = new(sync.Mutex)
	ego.globalLock.Unlock()

	ego.files.AddRecord(collection.RecordConf{
		Id: id,
		Cols: []collection.FielderConf{
			collection.FieldConf[uint64]{Value: uint64(parent.Id)},
			collection.FieldConf[[]string]{Value: []string(absPath)},
			collection.FieldConf[uint8]{Value: uint8(givenFlags)},
			collection.FieldConf[string]{Value: location},
			collection.FieldConf[time.Time]{Value: origTime},
			collection.FieldConf[time.Time]{Value: time.Now()},
		},
	})

	return

}

/*
Creates a directory (a file with topology).
If the file already exists, just adds the topology flag, otherwise creates a new one.

Parameters:
  - absPath - absolute path to the file,
  - origTime - time when the file was originally created.
*/
func (ego *localCountedStorageDriver) createDir(absPath fs.Path, origTime time.Time) (collection.CId, error) {

	rec, err := ego.findFile(absPath)
	if err != nil {
		return 0, err
	}

	if rec != nil {
		if rec.flags()&fs.FileTopology == 0 {
			rec.Cols[fieldFlags] = collection.FieldConf[uint8]{Value: uint8(rec.flags() | fs.FileTopology)}
			return rec.Id, ego.files.EditRecord(rec.conf())
		}
		return rec.Id, nil
	}

	if _, err := ego.createDir(absPath.Dir(), origTime); err != nil {
		return 0, err
	}
	id, err := ego.createFile(absPath, "", fs.FileTopology, origTime)
	return id, err

}

/*
Deletes a file (with all its descendants and their contents).

Parameters:
  - absPath - absolute path to the file.

Returns:
  - error if any occurred.
*/
func (ego *localCountedStorageDriver) deleteFile(absPath fs.Path) error {

	return ego.forFilesWithPrefix(absPath, func(rec record) error {

		for id := range ego.openFiles {
			if id == rec.Id {
				if err := ego.Close(absPath); err != nil {
					return err
				}
			}
		}

		if _, exists := ego.fileLocks[rec.Id]; !exists {
			ego.globalLock.Lock()
			delete(ego.fileLocks, rec.Id)
			ego.globalLock.Unlock()
		}

		if err := ego.files.DeleteRecord(collection.RecordConf{
			Id: rec.Id,
		}); err != nil {
			return err
		}

		if rec.flags()&fs.FileContent > 0 {
			if err := os.Remove(rec.location()); err != nil {
				return err
			}
		}

		return nil

	})

}

/*
Creates a new file from a record.

Parameters:
  - rec - file table record.

Returns:
  - file.
*/
func (ego *localCountedStorageDriver) newFile(rec record) fs.File {
	return fs.NewFile(fs.FileConf{
		Path:      rec.path(),
		StorageId: ego.id,
		Flags:     rec.flags(),
		OrigTime:  rec.origTime(),
	})
}

/*
Sets a new parent to a file.
Deletes the old file table record and creates a new one.

Parameters:
  - source - absolute path to the file,
  - dest - absolute path where the file should be moved (including the filename).

Returns:
  - error if any occurred.
*/
func (ego *localCountedStorageDriver) moveFile(source fs.Path, dest fs.Path) error {

	if rec, err := ego.findFile(dest); err != nil {
		return err
	} else if rec != nil {
		return errors.NewStateError(ego, errors.LevelError, "File of the same name already exists in the destination path.")
	}

	if rec, err := ego.findFile(source); err != nil {
		return err
	} else if rec == nil {
		return errors.NewNotFoundError(ego, errors.LevelError, `The file "`+source.Base()+`" does not exist.`)
	} else {
		if err := ego.files.DeleteRecord(collection.RecordConf{
			Id: rec.Id,
		}); err != nil {
			return err
		}
		parentId, err := ego.createDir(dest.Dir(), time.Now())
		if err != nil {
			return err
		}
		if _, err := ego.files.AddRecord(collection.RecordConf{
			Id: rec.Id,
			Cols: []collection.FielderConf{
				collection.FieldConf[uint64]{Value: uint64(parentId)},
				collection.FieldConf[[]string]{Value: []string(dest)},
				collection.FieldConf[uint8]{Value: uint8(rec.flags())},
				collection.FieldConf[string]{Value: rec.location()},
				collection.FieldConf[time.Time]{Value: rec.origTime()},
				collection.FieldConf[time.Time]{Value: time.Now()},
			},
		}); err != nil {
			return err
		}
	}

	return nil

}

/*
Creates a recursive copy of a file.
Opens both files, copies the data, creates a new file table record and recursively calls the method for the children of the file.

Parameters:
  - source - absolute path to the file,
  - parent - ID of the parent file,
  - dest - path where the file should be copied (including the filename).

Returns:
  - error if any occurred.
*/
func (ego *localCountedStorageDriver) copyFile(source fs.Path, parent collection.CId, dest fs.Path) error {

	if rec, err := ego.findFile(dest); err != nil {
		return err
	} else if rec != nil {
		return errors.NewStateError(ego, errors.LevelError, "File of the same name already exists in the destination path.")
	}

	rec, err := ego.findFile(source)
	if err != nil {
		return err
	}

	if rec == nil {
		return errors.NewNotFoundError(ego, errors.LevelError, `The file "`+source.Base()+`" does not exist.`)
	}

	// Opening the old file
	srcFd, err := ego.Open(source, fs.ModeRead, rec.flags(), rec.origTime())
	if err != nil {
		return err
	}
	defer ego.Close(source)

	// Creating a new location
	newLocation, err := ego.newLocation()
	if err != nil {
		return err
	}

	// Opening the new file
	dstFd, err := os.OpenFile(newLocation, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer dstFd.Close()

	// Copying the data
	dstFd.ReadFrom(srcFd)

	// A function for file creation
	create := func(content bool, path fs.Path, parent collection.CId, id collection.CId, location string) error {
		ego.files.AddRecord(collection.RecordConf{
			Id: id,
			Cols: []collection.FielderConf{
				collection.FieldConf[uint64]{Value: uint64(parent)},
				collection.FieldConf[[]string]{Value: []string(path)},
				collection.FieldConf[uint8]{Value: uint8(rec.flags())},
				collection.FieldConf[string]{Value: location},
				collection.FieldConf[time.Time]{Value: rec.origTime()},
				collection.FieldConf[time.Time]{Value: time.Now()},
			},
		})
		return nil
	}

	// Creating the parent directory (necessary only in on the highest level)
	if parent == 0 {
		if pid, err := ego.createDir(dest.Dir(), rec.origTime()); err != nil {
			return err
		} else {
			parent = pid
		}
	}

	ego.globalLock.Lock()

	ego.fileCount++
	newId := ego.fileCount

	ego.globalLock.Unlock()

	// Recursive call for each descendant
	ego.forFilesWithParent(rec.Id, func(r record) error {
		return ego.copyFile(r.path(), newId, dest.Join(fs.Path{r.path().Base()}))
	})

	return create(rec.flags()&fs.FileContent > 0, dest, parent, newId, rec.location())

}

/*
Exports files to a stream.
Iterates over all records in the file table and creates a file from each of those that satisfy the depth constraint.

Parameters:
  - absPath - where to begin the export,
  - depth - how many levels under the file specified by path to export (0 for only the file itself, 1 for LS).

Returns:
  - the readable stream with result,
  - error if any occurred.
*/
func (ego *localCountedStorageDriver) exportToStream(absPath fs.Path, depth fs.Depth) (stream.Producer[fs.File], error) {

	rec, err := ego.findFile(absPath)

	if err != nil {
		return nil, err
	} else if rec == nil {
		return nil, errors.NewNotFoundError(ego, errors.LevelError, "The path does not exist.")
	}

	pathLen := len(absPath)

	if s, err := ego.files.Filter(collection.FilterArgument{
		Limit: collection.NO_LIMIT,
		QueryConf: collection.QueryAtomConf{
			Name:      "path",
			Value:     []string(absPath),
			MatchType: collection.PrefixIndexConf[[]string]{},
		}}); err != nil {

		return nil, err

	} else {

		toRecord := stream.NewTransformer(func(conf collection.RecordConf) record { return record(conf) })
		filter := stream.NewFilter(func(rec record) bool { return fs.Depth(len(rec.path())-pathLen) <= depth })
		toFile := stream.NewTransformer(func(rec record) fs.File { return ego.newFile(rec) })

		s.Pipe(toRecord).(stream.Producer[record]).Pipe(filter).(stream.Producer[record]).Pipe(toFile)

		return toFile, nil

	}

}

/*
Closes a file.
Invokes closing of file descriptor, deletes the it from opened files and refreshes the modification time in the file table.

Parameters:
  - path - absolute path to the file.

Returns:
  - error if any occurred.
*/
func (ego *localCountedStorageDriver) closeFile(path fs.Path) error {

	rec, err := ego.findFile(path)
	if err != nil {
		return err
	}

	if rec == nil {
		return errors.NewNotFoundError(ego, errors.LevelError, `The file "`+path.String()+`" does not exist.`)
	}

	ego.globalLock.Lock()
	ego.openFiles[rec.Id].Close()
	delete(ego.openFiles, rec.Id)
	ego.fileLocks[rec.Id].Unlock()
	ego.globalLock.Unlock()
	rec.Cols[fieldModifTime] = collection.FieldConf[time.Time]{Value: time.Now()}
	if err := ego.files.EditRecord(rec.conf()); err != nil {
		return err
	}

	return nil
}

func (ego *localCountedStorageDriver) Open(path fs.Path, mode fs.FileMode, givenFlags fs.FileFlags, origTime time.Time) (fs.FileDescriptor, error) {

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
	var fid collection.CId

	// Checking if the file exists
	if rec, err := ego.findFile(path); err != nil {
		return nil, err
	} else if rec != nil {

		var err error
		location := rec.location()

		// If the file does not have content yet, addding it

		if rec.flags()&fs.FileContent == 0 {
			location, err = ego.newLocation()
			if err != nil {
				return nil, err
			}
			rec.Cols[fieldFlags] = collection.FieldConf[uint8]{Value: uint8(rec.flags() | fs.FileContent)}
			rec.Cols[fieldLocation] = collection.FieldConf[string]{Value: location}
			if err := ego.files.EditRecord(rec.conf()); err != nil {
				return nil, err
			}
		}

		fid = rec.Id

		// Creating a file mutex if does not exist
		if _, exists := ego.fileLocks[fid]; !exists {
			ego.globalLock.Lock()
			ego.fileLocks[fid] = new(sync.Mutex)
			ego.globalLock.Unlock()
		}

		// Locking the file mutex
		ego.fileLocks[fid].Lock()

		// Opening the existing file
		fd, err = os.OpenFile(location, modeFlags, 0664)
		if err != nil {
			return nil, err
		}

	} else {

		// If in read mode, error (the file cannot be created without write permission)
		if mode == fs.ModeRead {
			return nil, errors.NewNotFoundError(ego, errors.LevelError, `The file "`+path.String()+`" does not exist.`)
		}

		// Checking if the parent file exists, if not, creating it
		if len(path) > 0 {
			if _, err := ego.createDir(path.Dir(), origTime); err != nil {
				return nil, err
			}
		}

		// Creating a new file
		var err error
		fullpath, err := ego.newLocation()
		if err != nil {
			return nil, err
		}

		// Creating a file entry
		fid, err = ego.createFile(path, fullpath, givenFlags|fs.FileContent, origTime)
		if err != nil {
			return nil, err
		}

		// Locking the file mutex
		ego.fileLocks[fid].Lock()

		// Opening the file
		fd, err = os.OpenFile(fullpath, modeFlags, 0664)
		if err != nil {
			return nil, err
		}

	}

	ego.globalLock.Lock()
	ego.openFiles[fid] = fd
	ego.globalLock.Unlock()

	return &localFileDescriptor{
		fd: fd,
	}, nil

}

func (ego *localCountedStorageDriver) Close(path fs.Path) error {
	return ego.closeFile(path)
}

func (ego *localCountedStorageDriver) MkDir(path fs.Path, origTime time.Time) error {
	_, err := ego.createDir(path, origTime)
	return err
}

func (ego *localCountedStorageDriver) Copy(srcPath fs.Path, dstPath fs.Path) error {
	return ego.copyFile(srcPath, 0, dstPath)
}

func (ego *localCountedStorageDriver) Move(srcPath fs.Path, dstPath fs.Path) error {
	if srcPath.Equals(dstPath) {
		errors.NewStateError(ego, errors.LevelWarning, "The source and destination paths are equal.")
	}
	return ego.moveFile(srcPath, dstPath)
}

func (ego *localCountedStorageDriver) Delete(path fs.Path) error {
	return ego.deleteFile(path)
}

func (ego *localCountedStorageDriver) Tree(path fs.Path, depth fs.Depth) (stream.Producer[fs.File], error) {
	return ego.exportToStream(path, depth)
}

func (ego *localCountedStorageDriver) Size(path fs.Path) (uint64, error) {

	var fd *os.File

	rec, err := ego.findFile(path)
	if err != nil {
		return 0, err
	}

	ego.globalLock.Lock()
	if ofd, ok := ego.openFiles[rec.Id]; !ok {
		ego.globalLock.Unlock()
		descriptor, err := ego.Open(path, fs.ModeRead, fs.FileUndetermined, *new(time.Time))
		if err != nil {
			return 0, err
		}
		fd = descriptor.(*localFileDescriptor).fd
		defer ego.Close(path)
	} else {
		ego.globalLock.Unlock()
		fd = ofd
	}

	stat, err := fd.Stat()
	if err != nil {
		return 0, err
	}

	return uint64(stat.Size()), nil

}

func (ego *localCountedStorageDriver) Flags(path fs.Path) (fs.FileFlags, error) {
	if rec, err := ego.findFile(path); err != nil || rec == nil {
		return fs.FileUndetermined, err
	} else {
		return rec.flags(), nil
	}
}

func (ego *localCountedStorageDriver) Location(path fs.Path) (location string, err error) {

	rec, err := ego.findFile(path)

	if err == nil {

		if rec == nil {
			err = errors.NewNotFoundError(ego, errors.LevelError, "The file does not exist.")
		} else if rec.flags()&fs.FileContent == 0 {
			err = errors.NewStateError(ego, errors.LevelWarning, "The file does not have content.")
		} else {
			location = rec.location()
		}

	}

	return

}

func (ego *localCountedStorageDriver) Commit() error {
	return nil
}

func (ego *localCountedStorageDriver) Clear() error {

	ego.globalLock.Lock()
	defer ego.globalLock.Unlock()

	if err := ego.files.DeleteByFilter(collection.FilterArgument{
		Limit: collection.NO_LIMIT,
		QueryConf: collection.QueryAndConf{
			QueryContextConf: collection.QueryContextConf{Context: []collection.QueryConf{}},
		}}); err != nil {
		return err
	}

	for _, fd := range ego.openFiles {
		fd.Close()
	}
	ego.openFiles = make(map[collection.CId]*os.File)

	ego.createRoot()
	ego.locationCount = 0

	return os.RemoveAll(ego.prefix)

}

func (ego *localCountedStorageDriver) Features() fs.StorageFeatures {
	return fs.FeatureRead | fs.FeatureWrite | fs.FeatureLocation
}

func (ego *localCountedStorageDriver) Id() gonatus.GId {
	ego.globalLock.Lock()
	defer ego.globalLock.Unlock()
	return ego.id
}

func (ego *localCountedStorageDriver) SetId(id gonatus.GId) {
	ego.globalLock.Lock()
	defer ego.globalLock.Unlock()
	ego.id = id
}

func (ego *localCountedStorageDriver) Serialize() gonatus.Conf {
	return LocalCountedStorageConf{Prefix: ego.prefix}
}
