package gonatus

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/mitchellh/mapstructure"
	"golang.org/x/exp/slog"
)

type Conf map[string]any

func NewConf(class string) Conf {
	ego := Conf{}
	ego["CLASS"] = class
	return ego
}

func (ego Conf) unfold() {
	for key, value := range ego {
		ego[key] = unfold(value)
	}
}

func unfold(value any) any {
	nestedMap, isMap := value.(map[string]any)
	if isMap {
		for key, value := range nestedMap {
			nestedMap[key] = unfold(value)
		}
		return Conf(nestedMap)
	}
	nestedSlice, isSlice := value.([]any)
	if isSlice {
		for _, value := range nestedSlice {
			nestedSlice = append(nestedSlice, unfold(value))
		}
	}
	return value
}

func (ego Conf) Load(target Gobjecter) error {
	if ego != nil {
		if err := ego.Decode(target); err != nil {
			return err
		}
		target.setConf(ego.Clone())
	} else {
		return errors.New("Cannot load an empty Conf.")
	}
	return nil
}

func (ego Conf) Class() string {
	class, ok := ego["CLASS"].(string)
	if !ok {
		panic("The class property is not set.")
	}
	return class
}

func (ego Conf) Clone() Conf {
	new := Conf{}
	for key, value := range ego {
		new[key] = value
	}
	return new
}

func (ego Conf) Marshal() ([]byte, error) {
	return json.Marshal(ego)
}

func (ego Conf) Unmarshal(jsonBytes []byte) error {
	if err := json.Unmarshal(jsonBytes, &ego); err != nil {
		return err
	}
	ego.unfold()
	return nil
}

func (ego Conf) Encode(ptr any) error {
	obj := reflect.ValueOf(ptr).Elem()
	if err := mapstructure.Decode(obj.Interface(), &ego); err != nil {
		return err
	}
	ego.unfold()
	return nil
}

func (ego Conf) Decode(ptr any) error {
	return mapstructure.Decode(ego, ptr)
}

func (ego Conf) String() string {
	bytes, err := ego.Marshal()
	if err != nil {
		panic("Unable to serialize the conf.")
	}
	return string(bytes)
}

type Gobjecter interface {
	Init(ptr Gobjecter)
	Serialize() Conf
	Ptr() any
	setPtr(ptr Gobjecter)
	setConf(conf Conf)
	SetLog(*slog.Logger)
	Log() *slog.Logger
}

type Gobject struct {
	ptr   Gobjecter
	conf  Conf
	CLASS string
	log   *slog.Logger
}

func (ego *Gobject) Init(ptr Gobjecter) {
	ego.setPtr(ptr)
	if ego.conf == nil {
		className := reflect.TypeOf(ptr).Elem().Name()
		ego.setConf(NewConf(className))
		ego.CLASS = className
	}

	if ego.log == nil {
		ego.log = throwAwayLogger
	}

}

func (ego *Gobject) Serialize() Conf {

	conf := NewConf(reflect.TypeOf(ego.ptr).Elem().Name())
	err := conf.Encode(ego.ptr)
	if err != nil {
		panic(err)
	}
	return conf

}

func (ego *Gobject) Ptr() any {
	return ego.ptr
}

func (ego *Gobject) setPtr(ptr Gobjecter) {
	ego.ptr = ptr
}

func (ego *Gobject) setConf(conf Conf) {
	ego.conf = conf
}

// Log returns associated slog.Logger
// If the logger is not initialized (nil), a default slog.Logger which throws every log record away is returned instead
func (ego *Gobject) Log() *slog.Logger {
	if ego.log == nil {
		return throwAwayLogger
	} else {
		return ego.log
	}
}

// SetLog sets an associated slog.Logger of the Gobject
func (ego *Gobject) SetLog(log *slog.Logger) {
	ego.log = log
}

// throwawayHandler is a slog.Handler which does not process any Record (i.e. it throws everything away)
// this behaviour propagates to any derived handlers/loggers
type throwawayHandler struct{}

func (h throwawayHandler) Enabled(context.Context, slog.Level) bool {
	return false
}

func (h throwawayHandler) Handle(context.Context, slog.Record) error {
	return nil
}

func (h throwawayHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h throwawayHandler) WithGroup(name string) slog.Handler {
	return h
}

// throwAwayLogger is (a pointer to) a single instance of "throwAwayLogger", i.e. a loggger which does not log anything.
// As it is unexported and has no consturctor nor modifying methods, it is practically a singleton.
var throwAwayLogger = slog.New(throwawayHandler{})
