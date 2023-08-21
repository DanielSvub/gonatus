package gonatus

import (
	"github.com/SpongeData-cz/gonatus/logging"
	slog "golang.org/x/exp/slog"
)

type Conf any

type Gobjecter interface {
	Serialize() Conf
	SetLog(*slog.Logger)
	Log() *slog.Logger
}

type Gobject struct {
	log *slog.Logger // associated logger, always access it through Log() method, as it returns valid logger even for noninitialized gobjects
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
