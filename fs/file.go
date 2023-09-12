package fs

import (
	"io"
	"time"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
	"github.com/SpongeData-cz/stream"
)

/*
Flag byte for the file.
*/
type FileFlags uint8

const (
	FileUndetermined FileFlags = 0
	FileContent      FileFlags = 1 << (iota - 1)
	FileTopology
)

/*
Configuration structure for the file.
*/
type FileConf struct {
	Path      Path
	StorageId gonatus.GId
	Flags     FileFlags
	OrigTime  time.Time
}

/*
Stat structure for the file.
*/
type FileStat struct {
	Flags     FileFlags
	Size      uint64
	OrigTime  time.Time
	ModifTime time.Time
}

/*
Extends:
  - gonatus.Gobject.

Implements:
  - File.
*/
type file struct {
	gonatus.Gobject
	fd      FileDescriptor
	stat    FileStat
	path    Path
	storage Storage
}

/*
File constructor.

Parameters:
  - conf - configuration structure.

Returns:
  - pointer to the created file.
*/
func NewFile(conf FileConf) File {

	ego := new(file)

	ego.storage, _ = GStorageManager.Fetch(conf.StorageId)

	if conf.Path == nil {
		ego.path = Path{}
	} else {
		ego.path = conf.Path
	}

	now := time.Now()
	var origTime time.Time
	if conf.OrigTime.IsZero() {
		origTime = now
	} else {
		origTime = conf.OrigTime
	}

	ego.stat = FileStat{
		Flags:     conf.Flags,
		OrigTime:  origTime,
		ModifTime: now,
	}
	return ego
}

/*
Performs an inter-storage copy of the file.
If the source and destination files are in the same storage, same-storage copy should be used.

Parameters:
  - dst - destination file.

Returns:
  - error if any occurred.
*/
func (ego *file) interStorageCopy(dst File) error {

	var flags FileFlags
	if stat, err := ego.Stat(); err != nil {
		return err
	} else {
		flags = stat.Flags
	}

	if flags&FileContent > 0 {
		if err := ego.Open(ModeRead); err != nil {
			return err
		}
		defer ego.Close()
		dst.SetOrigTime(ego.stat.OrigTime)
		if err := dst.Open(ModeWrite); err != nil {
			return err
		}
		defer dst.Close()
		if _, err := dst.ReadFrom(ego); err != nil {
			return err
		}
	}

	if flags&FileTopology > 0 {
		if err := dst.MkDir(); err != nil {
			return err
		}
		ls, err := ego.Tree(DepthLs)
		if err != nil {
			return err
		}
		for !ls.Closed() {
			res := make([]File, 1)
			if n, err := ls.Read(res); err != nil {
				return err
			} else if n < 1 {
				continue
			} else {
				file := res[0].(*file)
				if file.path.Equals(ego.path) {
					continue
				}
				if err := file.Copy(NewFile(FileConf{
					Path:      dst.Path().Join(Path{file.Name()}),
					StorageId: dst.Storage().driver().Id(),
					Flags:     file.stat.Flags,
				})); err != nil {
					return err
				}
			}
		}
	}

	return nil

}

func (ego *file) Storage() Storage {
	return ego.storage
}

func (ego *file) Path() Path {
	return ego.path
}

func (ego *file) Location() (string, error) {
	return ego.storage.driver().Location(ego.path)
}

func (ego *file) Name() string {
	length := len(ego.path)
	if length == 0 {
		return ""
	}
	return ego.path[length-1]
}

func (ego *file) Copy(dst File) error {

	if ego.Storage() == nil {
		return errors.NewNilError(ego, errors.LevelError, "Storage not set.")
	}

	if ego.Storage().driver().Id() == dst.Storage().driver().Id() {
		if err := ego.Storage().driver().Copy(ego.Path(), dst.Path()); err != nil {
			return err
		}
	} else if err := ego.interStorageCopy(dst); err != nil {
		return err
	}

	dst.(*file).stat.ModifTime = time.Now()

	return nil
}

func (ego *file) Move(dst File) error {

	if ego.Storage() == nil {
		return errors.NewNilError(ego, errors.LevelError, "Storage not set.")
	}

	if ego.Storage().driver().Id() == dst.Storage().driver().Id() {

		if err := ego.Storage().driver().Move(ego.Path(), dst.Path()); err != nil {
			return err
		}

	} else if ego.Storage().driver().Features()&FeatureWrite == 0 {

		return errors.NewNotImplError(ego)

	} else if err := ego.interStorageCopy(dst); err != nil {

		return err

	} else if err := ego.Delete(); err != nil {

		return err

	}

	stat, err := dst.Stat()
	if err != nil {
		return err
	}

	ego.storage = dst.Storage()
	ego.path = dst.Path()
	ego.stat.Flags |= stat.Flags
	ego.stat.ModifTime = time.Now()

	return nil
}

func (ego *file) Delete() error {
	if ego.Storage() == nil {
		return errors.NewNilError(ego, errors.LevelError, "Storage not set.")
	}
	if err := ego.Storage().driver().Delete(ego.path); err != nil {
		return err
	}
	return nil
}

func (ego *file) Open(mode FileMode) error {

	if ego.fd != nil {
		return errors.NewStateError(ego, errors.LevelError, "The file is already open.")
	}

	if ego.Storage() == nil {
		return errors.NewNilError(ego, errors.LevelError, "Storage not set.")
	}

	fd, err := ego.Storage().driver().Open(ego.path, mode, ego.stat.Flags, ego.stat.OrigTime)
	if err != nil {
		return err
	}

	ego.fd = fd

	return nil

}

func (ego *file) MkDir() error {
	if ego.Storage() == nil {
		return errors.NewNilError(ego, errors.LevelError, "Storage not set.")
	}
	ego.stat.Flags |= FileTopology
	return ego.Storage().driver().MkDir(ego.path, ego.stat.OrigTime)
}

func (ego *file) Tree(depth Depth) (stream.Producer[File], error) {
	if ego.Storage() == nil {
		return nil, errors.NewNilError(ego, errors.LevelError, "Storage not set.")
	}
	return ego.Storage().driver().Tree(ego.path, depth)
}

func (ego *file) Stat() (FileStat, error) {
	if ego.Storage() == nil {
		return *new(FileStat), errors.NewNilError(ego, errors.LevelError, "Storage not set.")
	}
	var err error
	ego.stat.Flags, err = ego.Storage().driver().Flags(ego.path)
	if err != nil {
		return ego.stat, err
	}
	if ego.stat.Flags&FileContent > 0 {
		ego.stat.Size, err = ego.Storage().driver().Size(ego.path)
	}
	return ego.stat, err
}

func (ego *file) SetOrigTime(time time.Time) {
	ego.stat.OrigTime = time
}

func (ego *file) Read(p []byte) (n int, err error) {
	if ego.fd == nil {
		return 0, errors.NewStateError(ego, errors.LevelError, "File not open.")
	}
	return ego.fd.Read(p)
}

func (ego *file) ReadFrom(r io.Reader) (n int64, err error) {
	if ego.fd == nil {
		return 0, errors.NewStateError(ego, errors.LevelError, "File not open.")
	}
	return ego.fd.ReadFrom(r)
}

func (ego *file) ReadAt(p []byte, off int64) (n int, err error) {
	if ego.fd == nil {
		return 0, errors.NewStateError(ego, errors.LevelError, "File not open.")
	}
	return ego.fd.ReadAt(p, off)
}

func (ego *file) Write(p []byte) (n int, err error) {
	if ego.fd == nil {
		return 0, errors.NewStateError(ego, errors.LevelError, "File not open.")
	}
	return ego.fd.Write(p)
}

func (ego *file) Seek(offset int64, whence int) (int64, error) {
	if ego.fd == nil {
		return 0, errors.NewStateError(ego, errors.LevelError, "File not open.")
	}
	return ego.fd.Seek(offset, whence)
}

func (ego *file) Close() error {
	if ego.fd == nil {
		return errors.NewStateError(ego, errors.LevelError, "File not open.")
	}
	if ego.Storage() == nil {
		return errors.NewNilError(ego, errors.LevelError, "Storage not set.")
	}
	ego.stat.ModifTime = time.Now()
	if err := ego.Storage().driver().Close(ego.path); err != nil {
		return err
	}
	ego.fd = nil
	return nil
}

func (ego *file) Serialize() gonatus.Conf {

	out := *new(FileConf)

	out.Path = ego.path
	out.OrigTime = ego.stat.OrigTime
	if ego.storage != nil {
		out.StorageId = ego.Storage().driver().Id()
		out.Flags, _ = ego.Storage().driver().Flags(ego.path)
	}

	return out

}
