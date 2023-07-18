package drivers

import (
	"fmt"
	"io"
	"os"
	pathlib "path"
	"time"

	"github.com/SpongeData-cz/gonatus/collection"
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
	fieldOrigTime
	fieldModifTime
	fieldLocation
)

type record collection.RecordConf

func (ego *record) conf() collection.RecordConf {
	return collection.RecordConf(*ego)
}

func (ego *record) parent() collection.CId {
	return collection.CId(ego.Cols[fieldParent].(uint64))
}

func (ego *record) path() fs.Path {
	return ego.Cols[fieldPath].([]string)
}

func (ego *record) flags() fs.FileFlags {
	return fs.FileFlags(ego.Cols[fieldFlags].(uint8))
}

func (ego *record) origTime() time.Time {
	return ego.Cols[fieldOrigTime].(time.Time)
}

func (ego *record) modifTime() time.Time {
	return ego.Cols[fieldModifTime].(time.Time)
}

func (ego *record) location() string {
	return ego.Cols[fieldLocation].(string)
}

type LocalCountedStorageConf struct {
	Prefix string
}

type localCountedStorageDriver struct {
	gonatus.Gobject
	id            fs.StorageId
	prefix        string
	files         collection.Collection
	openFiles     map[collection.CId]*os.File
	fileCount     collection.CId
	locationCount uint64
}

func NewLocalCountedStorage(conf LocalCountedStorageConf) fs.Storage {
	ego := new(localCountedStorageDriver)
	ego.prefix = conf.Prefix
	ego.files = collection.NewRamCollection(collection.RamCollectionConf{
		SchemaConf: collection.SchemaConf{
			Name:         "FileTable",
			FieldsNaming: []string{"parent", "path", "flags", "origTime", "modifTime", "location"},
			Fields: []collection.FielderConf{
				collection.FieldConf[uint64]{},
				collection.FieldConf[[]string]{},
				collection.FieldConf[uint8]{},
				collection.FieldConf[time.Time]{},
				collection.FieldConf[time.Time]{},
				collection.FieldConf[string]{},
			},
			Indexes: [][]collection.IndexerConf{{collection.PrefixIndexConf[[]string]{Name: "path"}}},
		},
	})
	ego.openFiles = make(map[collection.CId]*os.File)
	ego.createRoot()
	return fs.NewStorage(ego)
}

func (ego *localCountedStorageDriver) createRoot() error {
	ego.fileCount = 1
	now := time.Now()
	_, err := ego.files.AddRecord(collection.RecordConf{
		Id: 1,
		Cols: []collection.FielderConf{
			collection.FieldConf[uint64]{Value: 0},
			collection.FieldConf[[]string]{Value: []string{}},
			collection.FieldConf[uint8]{Value: uint8(fs.FileTopology)},
			collection.FieldConf[time.Time]{Value: now},
			collection.FieldConf[time.Time]{Value: now},
		},
	})
	return err
}

/*
Acquires an ID of the file on the given path.

Parameters:
  - path - path to the file.

Returns:
  - ID of the found file, -1 if the path does not exist.
*/
func (ego *localCountedStorageDriver) findFile(path fs.Path) (*record, error) {

	if stream, err := ego.files.Filter(collection.QueryAtomConf{
		Name:      "path",
		Value:     []string(path),
		MatchType: collection.PrefixIndexConf[[]string]{},
	}); err != nil {
		return nil, err
	} else {
		if s, err := stream.Collect(); err != nil {
			return nil, err
		} else if len(s) >= 1 {
			rec := record(s[0])
			return &rec, nil
		}
	}

	return nil, nil

}

func (ego *localCountedStorageDriver) forEach(path fs.Path, fn func(record) error) error {
	if stream, err := ego.files.Filter(collection.QueryAtomConf{
		Name:      "path",
		Value:     []string(path),
		MatchType: collection.PrefixIndexConf[[]string]{},
	}); err != nil {
		return err
	} else {
		for !stream.Closed() {
			s := make([]collection.RecordConf, 1)
			if _, err := stream.Read(s); err != nil {
				return err
			}
			if err := fn(record(s[0])); err != nil {
				return err
			}
		}
	}
	return nil
}

/*
Creates a new counted storage location.
Increments the file counter and creates the directory tree in local file system, if it does not exist.

Returns:
  - location - destination fullpath (12 numbers splitted by 3 + ".bin"),
  - err - error if any occurred.
*/
func (ego *localCountedStorageDriver) newLocation() (location string, err error) {
	str := fmt.Sprintf("%012d", ego.locationCount)
	ego.locationCount++
	location = pathlib.Join(ego.prefix, str[:3]+"/"+str[3:6]+"/"+str[6:9])
	err = os.MkdirAll(location, os.ModePerm)
	location += "/" + str[9:] + ".bin"
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
func (ego *localCountedStorageDriver) createFile(path fs.Path, location string, givenFlags fs.FileFlags, origTime time.Time) (id collection.CId, err error) {

	parent, err := ego.findFile(path.Dir())
	if err != nil {
		return
	}

	if parent == nil {
		err = errors.NewNotFoundError(ego, errors.LevelError, "The path does not exist.")
		return
	}

	ego.fileCount++
	ego.files.AddRecord(collection.RecordConf{
		Id: collection.CId(ego.fileCount),
		Cols: []collection.FielderConf{
			collection.FieldConf[uint64]{Value: uint64(parent.Id)},
			collection.FieldConf[[]string]{Value: []string(path)},
			collection.FieldConf[uint8]{Value: uint8(givenFlags)},
			collection.FieldConf[time.Time]{Value: origTime},
			collection.FieldConf[time.Time]{Value: time.Now()},
			collection.FieldConf[string]{Value: location},
		},
	})

	return

}

/*
Deletes a file.

Parameters:
  - path - path to the file.

Returns:
  - error if any occurred.
*/
func (ego *localCountedStorageDriver) deleteFile(path fs.Path) error {

	return ego.forEach(path, func(rec record) error {

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

Parameters:
  - source - path to the file,
  - dest - path where the file should be moved.

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
		ego.files.DeleteRecord(collection.RecordConf{
			Id: rec.Id,
		})
		ego.files.AddRecord(collection.RecordConf{
			Id: rec.Id,
			Cols: []collection.FielderConf{
				collection.FieldConf[uint64]{Value: uint64(rec.parent())},
				collection.FieldConf[[]string]{Value: []string(rec.path())},
				collection.FieldConf[uint8]{Value: uint8(rec.flags())},
				collection.FieldConf[time.Time]{Value: rec.origTime()},
				collection.FieldConf[time.Time]{Value: rec.modifTime()},
			},
		})
	}

	return nil

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

	srcFd, err := ego.Open(source, fs.ModeRead, rec.flags(), rec.origTime())
	if err != nil {
		return err
	}
	defer ego.Close(source)

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

	create := func(content bool, path fs.Path, parent collection.CId, id collection.CId, location string) error {
		ego.files.AddRecord(collection.RecordConf{
			Id: id,
			Cols: []collection.FielderConf{
				collection.FieldConf[uint64]{Value: uint64(parent)},
				collection.FieldConf[[]string]{Value: []string(path)},
				collection.FieldConf[uint8]{Value: uint8(rec.flags())},
				collection.FieldConf[time.Time]{Value: rec.origTime()},
				collection.FieldConf[time.Time]{Value: time.Now()},
				collection.FieldConf[string]{Value: location},
			},
		})
		return nil
	}

	ego.fileCount++
	newId := ego.fileCount

	ego.forEach(source, func(r record) error {
		if rec.Id != r.Id {
			if err := ego.copyFile(r.path(), newId, dest); err != nil {
				return err
			}
		}
		return nil
	})

	if pid, err := ego.createDir(dest.Dir(), rec.origTime()); err != nil {
		return err
	} else {
		if parent == 0 {
			parent = pid
		}
		return create(rec.flags()&fs.FileContent > 0, dest, parent, newId, rec.location())
	}

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
func (ego *localCountedStorageDriver) exportToStream(path fs.Path, depth fs.Depth) (streams.ReadableOutputStreamer[fs.File], error) {

	if rec, err := ego.findFile(path); err != nil {
		return nil, err
	} else if rec == nil {
		return nil, errors.NewNotFoundError(ego, errors.LevelError, "The path does not exist.")
	}

	pathLen := len(path)

	inputStream := streams.NewBufferInputStream[fs.File](1)
	outputStream := streams.NewReadableOutputStream[fs.File]()
	inputStream.Pipe(outputStream)

	go func() {
		ego.forEach(path, func(rec record) error {
			if len(rec.path())-pathLen <= int(depth) {
				inputStream.Write(ego.newFile(rec))
			}
			return nil
		})
		inputStream.Close()
	}()

	return outputStream, nil

}

/*
Closes a file.

Parameters:
  - path - path to the file.

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

	ego.openFiles[rec.Id].Close()
	delete(ego.openFiles, rec.Id)
	ego.files.EditRecord(rec.conf(), fieldModifTime, time.Now())

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
			ego.files.EditRecord(rec.conf(), fieldFlags, rec.flags()|fs.FileContent)
			ego.files.EditRecord(rec.conf(), fieldLocation, location)
		}

		// Opening the existing file
		fd, err = os.OpenFile(location, modeFlags, 0664)
		if err != nil {
			return nil, err
		}

		fid = rec.Id

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

		// Creating a new file and opening it
		var err error
		fullpath, err := ego.newLocation()
		fd, err = os.OpenFile(fullpath, modeFlags, 0664)
		if err != nil {
			return nil, err
		}

		// Creating a file entry
		fid, err = ego.createFile(path, fullpath, givenFlags|fs.FileContent, origTime)
		if err != nil {
			return nil, err
		}

	}

	ego.openFiles[fid] = fd

	return &localFileDescriptor{
		fd: fd,
	}, nil

}

func (ego *localCountedStorageDriver) Close(path fs.Path) error {
	return ego.closeFile(path)
}

func (ego *localCountedStorageDriver) createDir(path fs.Path, origTime time.Time) (collection.CId, error) {

	rec, err := ego.findFile(path)
	if err != nil {
		return 0, err
	}

	if rec != nil {
		if rec.flags()&fs.FileTopology == 0 {
			return rec.Id, ego.files.EditRecord(rec.conf(), fieldFlags, rec.flags()|fs.FileTopology)
		}
		return rec.Id, nil
	}

	if _, err := ego.createDir(path.Dir(), origTime); err != nil {
		return 0, err
	}
	id, err := ego.createFile(path, "", fs.FileTopology, origTime)
	return id, err

}

func (ego *localCountedStorageDriver) MkDir(path fs.Path, origTime time.Time) error {
	_, err := ego.createDir(path, origTime)
	return err
}

func (ego *localCountedStorageDriver) Copy(srcPath fs.Path, dstPath fs.Path) error {
	if err := ego.copyFile(srcPath, 0, dstPath); err != nil {
		return err
	}
	return nil
}

func (ego *localCountedStorageDriver) Move(srcPath fs.Path, dstPath fs.Path) error {
	if !srcPath.Equals(dstPath) {
		if err := ego.moveFile(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

func (ego *localCountedStorageDriver) Delete(path fs.Path) error {
	return ego.deleteFile(path)
}

func (ego *localCountedStorageDriver) Tree(path fs.Path, depth fs.Depth) (streams.ReadableOutputStreamer[fs.File], error) {
	return ego.exportToStream(path, depth)
}

func (ego *localCountedStorageDriver) Size(path fs.Path) (uint64, error) {
	var fd *os.File
	rec, err := ego.findFile(path)
	if err != nil {
		return 0, err
	}
	if ofd, ok := ego.openFiles[rec.Id]; !ok {
		descriptor, err := ego.Open(path, fs.ModeRead, fs.FileUndetermined, *new(time.Time))
		if err != nil {
			return 0, err
		}
		fd = descriptor.(*localFileDescriptor).fd
		defer ego.Close(path)
	} else {
		fd = ofd
	}
	stat, err := fd.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(stat.Size()), nil
}

func (ego *localCountedStorageDriver) Flags(path fs.Path) (fs.FileFlags, error) {
	if rec, err := ego.findFile(path); err != nil {
		return fs.FileUndetermined, err
	} else if rec == nil {
		return fs.FileUndetermined, nil
	} else {
		return rec.flags(), nil
	}
}

func (ego *localCountedStorageDriver) Commit() error {
	return nil
}

func (ego *localCountedStorageDriver) Clear() error {
	if err := ego.files.DeleteByFilter(collection.QueryAndConf{
		QueryContextConf: collection.QueryContextConf{Context: []collection.QueryConf{}},
	}); err != nil {
		return err
	}
	ego.createRoot()
	ego.locationCount = 0
	return os.RemoveAll(ego.prefix)
}

func (ego *localCountedStorageDriver) Features() fs.StorageFeatures {
	return fs.FeatureRead | fs.FeatureWrite
}

func (ego *localCountedStorageDriver) Id() fs.StorageId {
	return ego.id
}

func (ego *localCountedStorageDriver) SetId(id fs.StorageId) {
	ego.id = id
}

func (ego *localCountedStorageDriver) Serialize() gonatus.Conf {
	return LocalCountedStorageConf{Prefix: ego.prefix}
}
