package driver

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/SpongeData-cz/gonatus/errors"
	"github.com/SpongeData-cz/gonatus/fs"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/streams"
)

type NativeStorageConf struct {
}

type nativeStorageDriver struct {
	gonatus.Gobject
	id      fs.StorageId
	opened  map[*os.File]fs.FileConf
	openedR map[string]*os.File
}

func NewNativeStorage(conf NativeStorageConf) fs.Storage {
	driver := new(nativeStorageDriver)
	driver.opened = make(map[*os.File]fs.FileConf)
	driver.openedR = make(map[string]*os.File)
	return fs.NewStorage(driver)
}

func (ego *nativeStorageDriver) PrintFAT() {
	print(errors.NewNotImplError(ego).Error())
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
	npath := strings.Join(path, "/")
	fd, err := os.OpenFile(npath, flags, 0664)
	if err != nil {
		return nil, err
	}

	ego.opened[fd] = fs.FileConf{
		Path:      path,
		StorageId: 0,
	}

	ego.openedR[npath] = fd

	// TODO: make sharable localFileDescriptor for fs based on local filesystem
	return &localFileDescriptor{
		fd: fd,
	}, nil
}

func (ego *nativeStorageDriver) Close(path fs.Path) error {
	npath := strings.Join(path, "/")
	fd := ego.openedR[npath]

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

func (ego nativeRecord) toFileConf() fs.FileConf {
	flags := fs.FileContent
	if ego.isDir {
		flags = fs.FileTopology
	}

	out := fs.FileConf{
		Path:  strings.Split(path.Dir(ego.path), "/"),
		Flags: flags,
	}

	return out
}

func nativeStat(path string) (bool, nativeRecord, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nativeRecord{}, err
	}

	me := nativeRecord{path: path}
	me.isDir = info.IsDir()

	if !me.isDir {
		me.size = uint64(info.Size())
	}

	return true, me, nil
}

func filterImpl(pfx string, depth int) ([]nativeRecord, error) {
	if depth < 0 {
		return make([]nativeRecord, 0), nil
	}

	info, err := os.Stat(pfx)
	if os.IsNotExist(err) {
		return nil, err
	}

	me := nativeRecord{path: pfx, isDir: info.IsDir()}
	accum := append(make([]nativeRecord, 0), me)

	if me.isDir {
		return accum, nil
	}

	items, _ := ioutil.ReadDir(pfx)

	for _, item := range items {
		children, err := filterImpl(item.Name(), depth-1)
		if err != nil {
			return nil, err
		}

		accum = append(accum, children...)
	}
	return accum, nil
}

func (ego *nativeStorageDriver) exportToStream(files []nativeRecord) (streams.ReadableOutputStreamer[fs.File], error) {
	export := func(stream streams.BufferInputStreamer[fs.File]) {
		for _, f := range files {
			flags := fs.FileContent
			if f.isDir {
				flags = fs.FileTopology
			}

			stream.Write(fs.NewFile(fs.FileConf{
				Path:  strings.Split(path.Dir(f.path), "/"),
				Flags: flags,
			}))
		}
		stream.Close()
	}

	inputStream := streams.NewBufferInputStream[fs.File](1)
	outputStream := streams.NewReadableOutputStream[fs.File]()

	inputStream.Pipe(outputStream)
	go export(inputStream)

	return outputStream, nil
}

func (ego *nativeStorageDriver) Tree(path fs.Path, depth fs.Depth) (streams.ReadableOutputStreamer[fs.File], error) {
	lst, err := filterImpl(strings.Join(path, "/"), int(depth))

	if err != nil {
		return nil, err
	}

	return ego.exportToStream(lst)
}

func filePathToNative(path fs.Path) string {
	return strings.Join(path, "/")
}

func (ego *nativeStorageDriver) Flags(path fs.Path) (fs.FileFlags, error) {
	valid, record, _ := nativeStat(filePathToNative(path))
	if !valid {
		return fs.FileUndetermined, nil
	}

	if record.isDir {
		return fs.FileTopology, nil
	}

	return fs.FileContent, nil
}

func (ego *nativeStorageDriver) Size(path fs.Path) (uint64, error) {
	valid, record, err := nativeStat(filePathToNative(path))

	if !valid {
		return 0, errors.NewNotFoundError(ego, errors.LevelError, "No such file or directory")
	}

	if err != nil {
		return 0, err
	}

	return record.size, nil
}

func (ego *nativeStorageDriver) Commit() error {
	return nil
}

func (ego *nativeStorageDriver) Clear() (err error) {
	return errors.NewNotImplError(ego)
}

func (ego *nativeStorageDriver) Id() fs.StorageId {
	return ego.id
}

func (ego *nativeStorageDriver) SetId(id fs.StorageId) {
	ego.id = id
}

func (ego *nativeStorageDriver) Serialize() gonatus.Conf {
	return nil
}
