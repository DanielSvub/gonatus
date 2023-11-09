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
The values are synchronized with the Slog library.
*/
type ErrorLevel int

const (
	LevelWrapper ErrorLevel = -8 // Not even an error, the actual one is wrapped inside.
	LevelInfo    ErrorLevel = 0  // Non-standard situation has occurred, but program continues normally.
	LevelWarning ErrorLevel = 4  // Something did not work out, alternative approach used.
	LevelError   ErrorLevel = 8  // Normal error level.
	LevelFatal   ErrorLevel = 12 // Most serious error, impossible to continue.
)

/*
Type of the error.
*/
type ErrorType string

const (
	TypeNA       ErrorType = "UndeterminedError"    // The type of the error was not specified
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
	return Wrap(serializeSource(src), "SourceWrapper", err)
}

/*
Creates a new Gonatus error.

Parameters:
  - conf - serialized error.

Returns:
  - created error.
*/
func New(conf ErrorConf) error {

	ego := gonatusError{
		errType:   conf.Type,
		msg:       conf.Msg,
		traceback: conf.Traceback,
		level:     conf.Level,
	}

	if ego.traceback != "" {
		ego.traced = true
	} else if conf.Level >= thresholdLevel {
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
Checks if the given error is a Gonatus error.

Parameters:
  - err - error to check.

Returns:
  - true if the error is a Gonatus error, false otherwise.
*/
func IsGonatusError(err error) bool {
	_, ok := err.(gonatusError)
	return ok
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

		var traceback string
		if gonatusErr.traced {
			traceback = gonatusErr.traceback
		}

		var wrapped []ErrorConf
		if gonatusErr.wrapped != nil {
			conf := Serialize(gonatusErr.wrapped)
			if conf != nil {
				wrapped = []ErrorConf{conf.(ErrorConf)}
			}
		}

		return ErrorConf{
			Type:      gonatusErr.errType,
			Level:     gonatusErr.level,
			Msg:       gonatusErr.msg,
			Traceback: traceback,
			Wrapped:   wrapped,
		}

	}

	return nil

}

type ErrorConf struct {
	Type      ErrorType   `json:"type"`
	Level     ErrorLevel  `json:"level"`
	Msg       string      `json:"msg"`
	Traceback string      `json:"traceback,omitempty"`
	Wrapped   []ErrorConf `json:"wrapped,omitempty"`
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
	if ego.wrapped == nil {
		msg += string(ego.errType)
		if len(ego.msg) > 0 {
			msg += ": "
		}
	}
	msg += ego.msg
	if ego.wrapped != nil {
		wrappedMsg := ego.wrapped.Error()
		if len(ego.msg) > 0 && len(wrappedMsg) > 0 {
			msg += ": "
		}
		msg += wrappedMsg
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
	return NewSrcWrapper(src, New(ErrorConf{TypeUnknown, LevelFatal, "", "", nil}))
}

/*
Creates a new source wrapper with a nil error.

Parameters:
  - src - source of the error.

Returns:
  - created error.
*/
func NewNilError(src gonatus.Gobjecter, level ErrorLevel, msg string) error {
	return NewSrcWrapper(src, New(ErrorConf{TypeNil, level, msg, "", nil}))
}

/*
Creates a new source wrapper with a value error.

Parameters:
  - src - source of the error.

Returns:
  - created error.
*/
func NewValueError(src gonatus.Gobjecter, level ErrorLevel, msg string) error {
	return NewSrcWrapper(src, New(ErrorConf{TypeValue, level, msg, "", nil}))
}

/*
Creates a new source wrapper with a state error.

Parameters:
  - src - source of the error.

Returns:
  - created error.
*/
func NewStateError(src gonatus.Gobjecter, level ErrorLevel, msg string) error {
	return NewSrcWrapper(src, New(ErrorConf{TypeState, level, msg, "", nil}))
}

/*
Creates a new source wrapper with a not found error.

Parameters:
  - src - source of the error.

Returns:
  - created error.
*/
func NewNotFoundError(src gonatus.Gobjecter, level ErrorLevel, msg string) error {
	return NewSrcWrapper(src, New(ErrorConf{TypeNotFound, level, msg, "", nil}))
}

/*
Creates a new source wrapper with a missapplication error.

Parameters:
  - src - source of the error.

Returns:
  - created error.
*/
func NewMisappError(src gonatus.Gobjecter, msg string) error {
	return NewSrcWrapper(src, New(ErrorConf{TypeMisapp, LevelError, msg, "", nil}))
}

/*
Creates a new source wrapper with a not implemented error.

Parameters:
  - src - source of the error.

Returns:
  - created error.
*/
func NewNotImplError(src gonatus.Gobjecter) error {
	return NewSrcWrapper(src, New(ErrorConf{TypeNotImpl, LevelFatal, "", "", nil}))
}
