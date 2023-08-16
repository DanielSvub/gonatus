package fs

import (
	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
	"github.com/SpongeData-cz/gonatus/fs"
	fsdriver "github.com/SpongeData-cz/gonatus/fs/driver"
	"github.com/SpongeData-cz/stream"
)

type StorageService interface {
	gonatus.Service
	Merge(srcId gonatus.GId, dstId gonatus.GId) error
	Tree(id gonatus.GId, depth fs.Depth) (stream.Producer[fs.File], error)
	ChDir(id gonatus.GId, path fs.Path) error
	Commit(id gonatus.GId) error
	Clear(id gonatus.GId) error
}

type StorageServiceConf struct{}

type storageService struct {
	gonatus.Gobject
	gonatus.DefaultService[fs.Storage]
}

func NewStorageService(conf StorageServiceConf) StorageService {
	ego := new(storageService)
	ego.DefaultService = *gonatus.NewDefaultService(ego, func(conf gonatus.Conf) (fs.Storage, error) {
		var storage fs.Storage
		switch storageConf := conf.(type) {
		case fsdriver.NativeStorageConf:
			storage = fsdriver.NewNativeStorage(storageConf)
		case fsdriver.LocalCountedStorageConf:
			storage = fsdriver.NewLocalCountedStorage(storageConf)
		default:
			return nil, errors.NewMisappError(ego, "Unknown conf.")
		}
		return storage, nil
	})
	return ego
}

func (ego *storageService) Merge(srcId gonatus.GId, dstId gonatus.GId) error {
	src := ego.Fetch(srcId).(fs.Storage)
	return ego.Fetch(dstId).(fs.Storage).Merge(src)
}

func (ego *storageService) Tree(id gonatus.GId, depth fs.Depth) (stream.Producer[fs.File], error) {
	return ego.Fetch(id).(fs.Storage).Tree(depth)
}

func (ego *storageService) ChDir(id gonatus.GId, path fs.Path) error {
	return ego.Fetch(id).(fs.Storage).ChDir(path)
}

func (ego *storageService) Commit(id gonatus.GId) error {
	return ego.Fetch(id).(fs.Storage).Commit()
}

func (ego *storageService) Clear(id gonatus.GId) error {
	return ego.Fetch(id).(fs.Storage).Clear()
}

func (ego *storageService) Serialize() gonatus.Conf {
	return StorageServiceConf{}
}
