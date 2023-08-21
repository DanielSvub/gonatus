package gonatus

import (
	"log/slog"

	"github.com/SpongeData-cz/gonatus/logging"
)

type GId uint64

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
If the logger is not initialized (nil), a DefaultLogger() is used instead

Returns:
  - logger.
*/
func (ego *Gobject) Log() *slog.Logger {
	if ego.log == nil {
		return logging.DefaultLogger()
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
