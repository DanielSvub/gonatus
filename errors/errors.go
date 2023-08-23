package errors

import (
	"encoding/json"
	"errors"
	"reflect"
	"runtime"
	"strconv"

	"github.com/SpongeData-cz/gonatus"
)

/*
Seriousness of the error.
*/
type ErrorLevel uint8

const (
	LevelFatal   ErrorLevel = iota // Most serious error, impossible to continue.
	LevelError                     // Normal error level.
	LevelWarning                   // Something did not work out, alternative approach used.
	LevelWrapper                   // Not even an error, the actual one is wrapped inside.
)

/*
Type of the error.
*/
type ErrorType string

const (
	TypeUnknown  ErrorType = "UnknownError"         // The program got into a state which should theoretically be impossible.
	TypeNil      ErrorType = "NilError"             // Value missing where expected.
	TypeValue    ErrorType = "ValueError"           // Value present but not valid.
	TypeState    ErrorType = "StateError"           // The program got into an incorrect state.
	TypeNotFound ErrorType = "NotFoundError"        // The required value could not be found.
	TypeMisapp   ErrorType = "MissapplicationError" // The function was incorrectly used by the user.
	TypeNotImpl  ErrorType = "NotImplementedError"  // The function is not implemented by this object.
)

const thresholdLevel = LevelError // Error level under which the traceback is created and source serialization is performed.

/*
Serializes the source Gobject into a JSON string.

Parameters:
  - object - object to serialize.

Returns:
  - JSON string.
*/
func serializeSource(object gonatus.Gobjecter) (msg string) {
	conf := object.Serialize()
	if conf != nil {
		confName := reflect.TypeOf(conf).Name()
		className := confName[:len(confName)-len(gonatus.ConfSuffix)]
		if json, err := json.Marshal(conf); err == nil {
			msg = className + string(json)
		}
	}
	return
}

/*
Wraps the given error with a wrapper containing information about the object which caused it.
If the error level is below the seriousness threshold, the text of the wrapper will be empty.

Parameters:
  - src - Gobject which called the function,
  - err - error to wrap.

Returns:
  - created error.
*/
func NewSrcWrapper(src gonatus.Gobjecter, err error) error {
	var srcMsg string
	if err.(gonatusError).level <= thresholdLevel {
		srcMsg = serializeSource(src)
	}
	return Wrap(srcMsg, "SourceWrapper", err)
}

/*
Creates a new Gonatus error.

Parameters:
  - errType - type of the error,
  - level - how serious the error is,
  - msg - what happened.

Returns:
  - created error.
*/
func New(conf ErrorConf) error {

	fullMsg := string(conf.ErrType)
	if len(conf.Msg) > 0 {
		fullMsg += ": " + conf.Msg
	}

	ego := gonatusError{
		errType: conf.ErrType,
		msg:     fullMsg,
		level:   conf.Level,
	}

	if conf.Level <= thresholdLevel {
		ego.createTraceback()
	}

	if conf.Wrapped != nil && len(conf.Wrapped) > 0 {
		ego.wrapped = New(conf.Wrapped[0])
	}

	return ego

}

/*
Calls the standard Join function.
Creates an error that wraps the given errors.

Parameters:
  - errs - any number of errors to join.

Returns:
  - new error.
*/
func Join(errs ...error) error {
	return errors.Join(errs...)
}

/*
Calls the standard Is function.
Is reports whether any error in err's tree matches target.

Parameters:
  - err - error to search in,
  - target - target error.

Returns:
  - true if a match is found, false otherwise.
*/
func Is(err error, target error) bool {
	return errors.Is(err, target)
}

/*
Calls the standard As function.
As finds the first error in err's tree that matches target, and if one is found, sets target to that error value.

Parameters:
  - err - error to search in,
  - target - target error.

Returns:
  - true if a match is found, false otherwise.
*/
func As(err error, target any) bool {
	return errors.As(err, target)
}

/*
Checks if the given error is of the given error type.

Parameters:
  - err - error to check,
  - errType - type to check.

Returns:
  - true if the error is of the type, false otherwise.
*/
func OfType(err error, errType ErrorType) bool {
	gonatusError, ok := err.(gonatusError)
	if !ok {
		return false
	}
	return gonatusError.errType == errType
}

/*
Creates a new error which wraps the given error.
The new error has the wrapper level.

Parameters:
  - msg - wrapper message,
  - errType - type of the wrapper,
  - wrapped - errror to wrap.

Returns:
  - new error.
*/
func Wrap(msg string, errType ErrorType, wrapped error) error {
	return gonatusError{
		msg:     msg,
		errType: errType,
		level:   LevelWrapper,
		wrapped: wrapped,
	}
}

/*
Acquires a error wrapped inside the given error.
If the given error is not a Gonatus error, the standard Unwrap mehod is called.

Parameters:
  - err - error to unwrap.

Returns:
  - wrapped error, nil if there is not one.
*/
func Unwrap(err error) error {
	wrapper, ok := err.(gonatusError)
	if !ok {
		return errors.Unwrap(err)
	}
	return wrapper.wrapped
}

/*
Acquires a traceback of the given error.
If no traceback was created, message "No traceback." is returned.

Parameters:
  - err - error to trace.

Returns:
  - traceback.
*/
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

func Serialize(err error) gonatus.Conf {

	gonatusErr, ok := err.(gonatusError)

	if ok {

		var wrapped []ErrorConf

		if gonatusErr.wrapped != nil {
			conf := Serialize(gonatusErr.wrapped)
			if conf != nil {
				wrapped = []ErrorConf{conf.(ErrorConf)}
			}
		}

		return ErrorConf{
			ErrType: gonatusErr.errType,
			Level:   gonatusErr.level,
			Msg:     gonatusErr.msg,
			Wrapped: wrapped,
		}

	}

	return nil

}

type ErrorConf struct {
	ErrType ErrorType
	Level   ErrorLevel
	Msg     string
	Wrapped []ErrorConf
}

/*
Gonatus error structure.

Implements:
  - error.
*/
type gonatusError struct {
	errType   ErrorType
	level     ErrorLevel
	msg       string
	traced    bool
	traceback string
	wrapped   error
}

/*
Acquires the error message.

Returns:
  - the text of the error.
*/
func (ego gonatusError) Error() (msg string) {
	msg = ego.msg
	if ego.wrapped != nil {
		msg += ": " + ego.wrapped.Error()
	}
	return
}

/*
Creates a traceback for the error and saves it as string.
The zero caller is this method itself, the first is an error constructor.
*/
func (ego *gonatusError) createTraceback() {
	for i := 2; true; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			break
		}
		if i > 2 {
			ego.traceback += "\n"
		}
		ego.traceback += fn.Name() + " (" + file + ":" + strconv.Itoa(line) + ")"
	}
	ego.traced = true
}

/*
Creates a new source wrapper with an unknown error.

Parameters:
  - src - source of the error.

Returns:
  - created error.
*/
func NewUnknownError(src gonatus.Gobjecter) error {
	return NewSrcWrapper(src, New(ErrorConf{TypeUnknown, LevelFatal, "", nil}))
}

/*
Creates a new source wrapper with a nil error.

Parameters:
  - src - source of the error.

Returns:
  - created error.
*/
func NewNilError(src gonatus.Gobjecter, level ErrorLevel, msg string) error {
	return NewSrcWrapper(src, New(ErrorConf{TypeNil, level, msg, nil}))
}

/*
Creates a new source wrapper with a value error.

Parameters:
  - src - source of the error.

Returns:
  - created error.
*/
func NewValueError(src gonatus.Gobjecter, level ErrorLevel, msg string) error {
	return NewSrcWrapper(src, New(ErrorConf{TypeValue, level, msg, nil}))
}

/*
Creates a new source wrapper with a state error.

Parameters:
  - src - source of the error.

Returns:
  - created error.
*/
func NewStateError(src gonatus.Gobjecter, level ErrorLevel, msg string) error {
	return NewSrcWrapper(src, New(ErrorConf{TypeState, level, msg, nil}))
}

/*
Creates a new source wrapper with a not found error.

Parameters:
  - src - source of the error.

Returns:
  - created error.
*/
func NewNotFoundError(src gonatus.Gobjecter, level ErrorLevel, msg string) error {
	return NewSrcWrapper(src, New(ErrorConf{TypeNotFound, level, msg, nil}))
}

/*
Creates a new source wrapper with a missapplication error.

Parameters:
  - src - source of the error.

Returns:
  - created error.
*/
func NewMisappError(src gonatus.Gobjecter, msg string) error {
	return NewSrcWrapper(src, New(ErrorConf{TypeMisapp, LevelError, msg, nil}))
}

/*
Creates a new source wrapper with a not implemented error.

Parameters:
  - src - source of the error.

Returns:
  - created error.
*/
func NewNotImplError(src gonatus.Gobjecter) error {
	return NewSrcWrapper(src, New(ErrorConf{TypeNotImpl, LevelFatal, "", nil}))
}
