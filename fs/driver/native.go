package driver

import (
	"os"
	pathlib "path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/DanielSvub/gonatus/errors"
	"github.com/DanielSvub/gonatus/fs"
	"github.com/DanielSvub/stream"

	"github.com/DanielSvub/gonatus"
)

type NativeStorageConf struct {
	Prefix string
}

type nativeStorageDriver struct {
	gonatus.Gobject
	id         gonatus.GId
	prefix     string
	opened     map[*os.File]fs.FileConf
	openedR    map[string]*os.File
	globalLock sync.Mutex
}

func NewNativeStorage(conf NativeStorageConf) fs.Storage {
	driver := new(nativeStorageDriver)
	if conf.Prefix == "" {
		driver.prefix = "/"
	} else {
		driver.prefix, _ = filepath.Abs(conf.Prefix)
	}
	driver.opened = make(map[*os.File]fs.FileConf)
	driver.openedR = make(map[string]*os.File)
	return fs.NewStorage(driver)
}

func (ego *nativeStorageDriver) nativePath(path fs.Path) string {
	return pathlib.Join(ego.prefix, strings.Join(path, "/"))
}

func (ego *nativeStorageDriver) storagePath(path string) fs.Path {
	pfxLen := len(strings.Split(ego.prefix, "/"))
	return strings.Split(path, "/")[pfxLen:]
}

func (ego *nativeStorageDriver) Open(path fs.Path, mode fs.FileMode, givenFlags fs.FileFlags, origTime time.Time) (fs.FileDescriptor, error) {
	// Creating flags
	var flags int
	switch mode {
	case fs.ModeRead:
		flags = os.O_RDONLY
	case fs.ModeWrite, fs.ModeAppend, fs.ModeRW:
		return nil, errors.NewNotImplError(ego)
	default:
		return nil, errors.NewMisappError(ego, "Invalid opening mode.")
	}

	// Opening the existing file
	npath := ego.nativePath(path)
	fd, err := os.OpenFile(npath, flags, 0664)
	if err != nil {
		return nil, err
	}

	ego.globalLock.Lock()

	ego.opened[fd] = fs.FileConf{
		Path:      path,
		StorageId: ego.id,
	}

	ego.openedR[npath] = fd

	ego.globalLock.Unlock()

	// TODO: make sharable localFileDescriptor for fs based on local filesystem
	return &localFileDescriptor{
		fd: fd,
	}, nil
}

func (ego *nativeStorageDriver) CloseDescriptor(_ fs.FileDescriptor, _ fs.Path) error {
	return nil
}

func (ego *nativeStorageDriver) CloseFile(path fs.Path) error {
	ego.globalLock.Lock()
	fd := ego.openedR[ego.nativePath(path)]
	ego.globalLock.Unlock()
	return fd.Close()
}

func (ego *nativeStorageDriver) MkDir(path fs.Path, origTime time.Time) error {
	return errors.NewNotImplError(ego)
}

func (ego *nativeStorageDriver) Copy(srcPath fs.Path, dstPath fs.Path) error {
	return errors.NewNotImplError(ego)
}

func (ego *nativeStorageDriver) Move(srcPath fs.Path, dstPath fs.Path) error {
	return errors.NewNotImplError(ego)
}

func (ego *nativeStorageDriver) Delete(path fs.Path) error {
	return errors.NewNotImplError(ego)
}

type nativeRecord struct {
	path  string
	isDir bool
	size  uint64
}

func nativeStat(path string) (bool, nativeRecord, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, nativeRecord{}, err
	}

	me := nativeRecord{path: path}
	me.isDir = info.IsDir()

	if !me.isDir {
		me.size = uint64(info.Size())
	}

	return true, me, nil
}

func filterImpl(pfx string, depth fs.Depth) (accum []nativeRecord, err error) {

	info, err := os.Stat(pfx)
	if os.IsNotExist(err) {
		return
	}

	me := nativeRecord{path: pfx, isDir: info.IsDir()}
	accum = append(make([]nativeRecord, 0), me)

	if !me.isDir || depth <= 0 {
		return
	}

	items, _ := os.ReadDir(pfx)

	for _, item := range items {
		children, err := filterImpl(pathlib.Join(pfx, item.Name()), depth-1)
		if err != nil {
			return nil, err
		}

		accum = append(accum, children...)
	}

	return
}

func (ego *nativeStorageDriver) exportToStream(files []nativeRecord) (stream.Producer[fs.File], error) {
	export := func(s stream.ChanneledInput[fs.File]) {
		defer s.Close()
		for _, f := range files {
			flags := fs.FileContent
			if f.isDir {
				flags = fs.FileTopology
			}

			s.Write(fs.NewFile(fs.FileConf{
				StorageId: ego.id,
				Path:      ego.storagePath(f.path),
				Flags:     flags,
			}))
		}
	}

	s := stream.NewChanneledInput[fs.File](1)

	go export(s)

	return s, nil
}

func (ego *nativeStorageDriver) Tree(path fs.Path, depth fs.Depth) (stream.Producer[fs.File], error) {
	lst, err := filterImpl(ego.nativePath(path), depth)

	if err != nil {
		return nil, err
	}

	return ego.exportToStream(lst)
}

func (ego *nativeStorageDriver) Flags(path fs.Path) (fs.FileFlags, error) {
	valid, record, err := nativeStat(ego.nativePath(path))
	if !valid {
		return fs.FileUndetermined, err
	}

	if record.isDir {
		return fs.FileTopology, nil
	}

	return fs.FileContent, nil
}

func (ego *nativeStorageDriver) Location(path fs.Path) (string, error) {
	return ego.nativePath(path), nil
}

func (ego *nativeStorageDriver) Size(path fs.Path) (uint64, error) {
	valid, record, err := nativeStat(ego.nativePath(path))

	if !valid {
		return 0, errors.NewNotFoundError(ego, errors.LevelError, "No such file or directory")
	}

	if err != nil {
		return 0, err
	}

	return record.size, nil
}

func (ego *nativeStorageDriver) Commit() error {
	return errors.NewNotImplError(ego)
}

func (ego *nativeStorageDriver) Clear() (err error) {
	return errors.NewNotImplError(ego)
}

func (ego *nativeStorageDriver) Features() fs.StorageFeatures {
	return fs.FeatureRead | fs.FeatureLocation
}

func (ego *nativeStorageDriver) Id() gonatus.GId {
	return ego.id
}

func (ego *nativeStorageDriver) SetId(id gonatus.GId) {
	ego.id = id
}

func (ego *nativeStorageDriver) Serialize() gonatus.Conf {
	return NativeStorageConf{Prefix: ego.prefix}
}
