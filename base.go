package gonatus

import (
	"context"

	slog "golang.org/x/exp/slog"
)

type Conf any

type Gobjecter interface {
	Serialize() Conf
	SetLog(*slog.Logger)
	Log() *slog.Logger
}

type Gobject struct {
	log *slog.Logger
}

/*
Log returns associated slog.Logger.
If the logger is not initialized (nil), a default slog.Logger which throws every log record away is returned instead.

Returns:
  - logger.
*/
func (ego *Gobject) Log() *slog.Logger {
	if ego.log == nil {
		return throwAwayLogger
	} else {
		return ego.log
	}
}

/*
SetLog sets an associated slog.Logger of the Gobject.

Parameters:
- logger.
*/
func (ego *Gobject) SetLog(log *slog.Logger) {
	ego.log = log
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

/*
throwAwayLogger is (a pointer to) a single instance of "throwAwayLogger", i.e. a loggger which does not log anything.
As it is unexported and has no consturctor nor modifying methods, it is practically a singleton.
*/
var throwAwayLogger = slog.New(throwawayHandler{})
