package errors

import (
	"encoding/json"
	"errors"
	"reflect"
	"runtime"
	"strconv"

	"github.com/SpongeData-cz/gonatus"
)

type ErrorLevel uint8

const (
	LevelFatal ErrorLevel = iota
	LevelError
	LevelWarning
	LevelWrapper
)

type ErrorType string

const (
	TypeUnknown  ErrorType = "UnknownError"
	TypeNil      ErrorType = "NilError"
	TypeValue    ErrorType = "ValueError"
	TypeState    ErrorType = "StateError"
	TypeNotFound ErrorType = "NotFoundError"
	TypeMisapp   ErrorType = "MissapplicationError"
	TypeNotImpl  ErrorType = "NotImplementedError"
)

const (
	confSuffix    = "Conf"
	tresholdLevel = LevelError
)

func serializeSource(object gonatus.Gobjecter) (msg string) {
	conf := object.Serialize()
	if conf != nil {
		confName := reflect.TypeOf(conf).Name()
		className := confName[:len(confName)-len(confSuffix)]
		if json, err := json.Marshal(conf); err == nil {
			msg = className + string(json) + ": "
		}
	}
	return
}

func NewSrcWrapper(src gonatus.Gobjecter, err error) error {
	var srcMsg string
	if err.(gonatusError).level <= tresholdLevel {
		srcMsg = serializeSource(src)
	}
	return Wrap(srcMsg, "SourceWrapper", err)
}

func New(errType ErrorType, level ErrorLevel, msg string) error {
	ego := gonatusError{
		errType: errType,
		msg:     string(errType) + ": " + msg,
		level:   level,
	}
	if level <= tresholdLevel {
		ego.createTraceback()
	}
	return ego
}

func Join(errs ...error) error {
	return errors.Join(errs...)
}

func Is(err error, target error) bool {
	return errors.Is(err, target)
}

func As(err error, target any) bool {
	return errors.As(err, target)
}

func OfType(err error, errType ErrorType) bool {
	gonatusError, ok := err.(gonatusError)
	if !ok {
		return false
	}
	return gonatusError.errType == errType
}

func Wrap(msg string, errType ErrorType, wrapped error) error {
	return gonatusError{
		msg:     msg,
		errType: errType,
		level:   LevelWrapper,
		wrapped: wrapped,
	}
}

func Unwrap(err error) error {
	wrapper, ok := err.(gonatusError)
	if !ok {
		return errors.Unwrap(err)
	}
	return wrapper.wrapped
}

func Traceback(err error) string {
	gonatusErr, ok := err.(gonatusError)
	if ok {
		if gonatusErr.traced {
			return gonatusErr.traceback
		}
		return Traceback(gonatusErr.wrapped)
	}
	return "No traceback."
}

type gonatusError struct {
	errType   ErrorType
	level     ErrorLevel
	msg       string
	traced    bool
	traceback string
	wrapped   error
}

func (ego gonatusError) Error() (msg string) {
	msg = ego.msg
	if ego.wrapped != nil {
		msg += ego.wrapped.Error()
	}
	return
}

func (ego *gonatusError) createTraceback() {
	for i := 3; true; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			break
		}
		if i > 3 {
			ego.traceback += "\n"
		}
		ego.traceback += fn.Name() + " (" + file + ":" + strconv.Itoa(line) + ")"
	}
	ego.traced = true
}

func NewUnknownError(src gonatus.Gobjecter) error {
	return NewSrcWrapper(src, New(TypeUnknown, LevelFatal, ""))
}

func NewNilError(src gonatus.Gobjecter, level ErrorLevel, msg string) error {
	return NewSrcWrapper(src, New(TypeNil, level, msg))
}

func NewValueError(src gonatus.Gobjecter, level ErrorLevel, msg string) error {
	return NewSrcWrapper(src, New(TypeValue, level, msg))
}

func NewStateError(src gonatus.Gobjecter, level ErrorLevel, msg string) error {
	return NewSrcWrapper(src, New(TypeState, level, msg))
}

func NewNotFoundError(src gonatus.Gobjecter, level ErrorLevel, msg string) error {
	return NewSrcWrapper(src, New(TypeNotFound, level, msg))
}

func NewMisappError(src gonatus.Gobjecter, msg string) error {
	return NewSrcWrapper(src, New(TypeMisapp, LevelError, msg))
}

func NewNotImplError(src gonatus.Gobjecter) error {
	return NewSrcWrapper(src, New(TypeNotImpl, LevelFatal, ""))
}
