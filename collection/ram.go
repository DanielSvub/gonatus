package collection

import (
	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/errors"
)

const MaxUint = ^uint(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1)
const MinInt = -MaxInt - 1

type RamCollectionConf struct {
	SchemaConf
	MaxMemory uint64
}

type RamCollection struct {
	gonatus.Gobject
	param         RamCollectionConf
	autoincrement CId
	rows          map[CId][]any
	indexes       map[string]map[any]CId
}

func NewRamCollection(rc RamCollectionConf) *RamCollection {
	ego := new(RamCollection)
	ego.param = rc
	ego.rows = make(map[CId][]any, 0)
	ego.indexes = make(map[string]map[any]CId, 0)

	return ego
}

func (ego *RamCollection) InterpretField(fc FielderConf) (any, error) {
	switch v := fc.(type) {
	case FieldStringConf:
		return v, nil
	default:
		return nil, errors.NewNotImplError(ego)
	}
}

func (ego *RamCollection) InterpretRecord(rc *Record) ([]any, error) {
	ret := make([]any, 0)

	for c := range rc.Cols {
		rf, err := ego.InterpretField(c)
		if err != nil {
			return nil, err
		}

		ret = append(ret, rf)
	}

	return ret, nil

}

func (ego *RamCollection) AddRecord(rc *Record) (CId, error) {
	ego.autoincrement++

	if ego.autoincrement == CId(MaxUint) {
		return 0, errors.NewValueError(ego, errors.LevelFatal, "Id pool depleted!")
	}

	return 0, nil
}

func (ego *RamCollection) Serialize() gonatus.Conf {
	return ego.param
}
