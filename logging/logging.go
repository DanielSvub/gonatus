package logging

import (
	"context"
	"errors"
	"io"
	"os"

	"log/slog"
)

// defaultLogger holds logger to which every gobject with unset logger fallbacks
var defaultLogger slog.Logger = *NewDebugStdOutLogger() //TODO this is to be discussed: what logger should we use as default? Currently: debug, stdout, with source

// DefaultLogger is a logger to which every gobject fallbacks if it's Log is not set by user
func DefaultLogger() *slog.Logger {
	return &defaultLogger
}

/*
SetDefaultLogger sets the DefaultLogger for every gobject with unset logger.
Returns error on nil.
*/
func SetDefaultLogger(logger *slog.Logger) error {
	if logger == nil {
		return errors.New("Default logger can not be nil.")
	}
	defaultLogger = *logger
	return nil

}

/*
throwawayHandler is a slog.Handler which does not process any Record (i.e. it throws everything away).
This behaviour propagates to any derived handlers/loggers.
*/
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

// NewThrowAwayLogger creates new logger which throws everything away, i.e. a loggger which does not log anything.
// This behaviour propagates to any derived loggers.
func NewThrowAwayLogger() *slog.Logger {
	return slog.New(throwawayHandler{})
}

// NewTextLogger creates a new logger which logs text messages of at least the level severity into the given writer.
func NewTextLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level}))
}

// NewJSONLogger creates a new logger which logs structured messages (JSON) of at least the level severity into the given writer.
func NewJSONLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

// NewDebugStdOutLogger creates new logger which logs text messages into the standard output with Debug level while also computing source of each message.
func NewDebugStdOutLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{AddSource: true, Level: slog.LevelDebug.Level()}))
}

// NewDebugFileLogger creates new logger which logs text messages into the given file with Debug level while also computing source of each message.
func NewDebugFileLogger(f *os.File) *slog.Logger {
	return slog.New(slog.NewTextHandler(f, &slog.HandlerOptions{AddSource: true, Level: slog.LevelDebug.Level()}))
}
