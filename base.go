package gonatus

import (
	"github.com/SpongeData-cz/gonatus/logging"
	slog "golang.org/x/exp/slog"
)

type Conf any

type GId uint64

type Gobjecter interface {
	Id() GId
	SetId(id GId)
	Serialize() Conf
	SetLog(*slog.Logger)
	Log() *slog.Logger
}

type Gobject struct {
	id  GId
	log *slog.Logger
}

func (ego *Gobject) Id() GId {
	return ego.id
}

func (ego *Gobject) SetId(id GId) {
	ego.id = id
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
