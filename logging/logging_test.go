package logging_test

import (
	"strings"
	"testing"

	"github.com/SpongeData-cz/gonatus"
	"github.com/SpongeData-cz/gonatus/logging"
	"golang.org/x/exp/slog"
)

//INFO: purpose is just to test correctness of work (setup, switch, ...)  with loggers, not correctnes of logging facility - this has already been done by slog developers

func TestLogging(t *testing.T) {

	t.Run("SetDefaultLogger", func(t *testing.T) {
		obj := gonatus.Gobject{}
		err := logging.SetDefaultLogger(nil)
		if err == nil {
			t.Error("Error expected.")
		}
		sb := &strings.Builder{}
		err = logging.SetDefaultLogger(logging.NewTextLogger(sb, slog.LevelDebug.Level()))
		if err != nil {
			t.Error("unexpected error")
		}
		obj.Log().Debug("debug message")
		logLines := strings.Split(sb.String(), "\n")
		if len(logLines) != 2 {
			t.Error("Unexpected log length", len(logLines))
		}
		if !strings.Contains(logLines[0], "msg=\"debug message\"") {
			t.Error("Unexpected log content", logLines[0])
		}

	})

	t.Run("DebugTextLog", func(t *testing.T) {
		obj := gonatus.Gobject{}
		sb := &strings.Builder{}
		err := logging.SetDefaultLogger(logging.NewTextLogger(sb, slog.LevelDebug.Level()))
		if err != nil {
			t.Error("unexpected error")
		}
		obj.Log().Debug("debug")
		obj.Log().Info("info")
		obj.Log().Warn("warn")
		obj.Log().Error("error")
		logLines := strings.Split(sb.String(), "\n")
		if len(logLines) != 5 {
			t.Error("Unexpected log length", len(logLines), "\n", logLines)
		}
		sb = &strings.Builder{}
		err = logging.SetDefaultLogger(logging.NewTextLogger(sb, slog.LevelError.Level()))
		if err != nil {
			t.Error("unexpected error")
		}
		obj.Log().Debug("debug")
		obj.Log().Info("info")
		obj.Log().Warn("warn")
		obj.Log().Error("error")

		logLines = strings.Split(sb.String(), "\n")
		if len(logLines) != 2 {
			t.Error("Unexpected log length", len(logLines), "\n", logLines)
		}

	})

	t.Run("DebugJSONLog", func(t *testing.T) {
		obj := gonatus.Gobject{}
		sb := &strings.Builder{}
		err := logging.SetDefaultLogger(logging.NewTextLogger(sb, slog.LevelDebug.Level()))
		if err != nil {
			t.Error("unexpected error")
		}
		obj.Log().Debug("debug")
		obj.Log().Info("info")
		obj.Log().Warn("warn")
		obj.Log().Error("error")
		logLines := strings.Split(sb.String(), "\n")
		if len(logLines) != 5 {
			t.Error("Unexpected log length", len(logLines), "\n", logLines)
		}
		sb = &strings.Builder{}
		err = logging.SetDefaultLogger(logging.NewTextLogger(sb, slog.LevelError.Level()))
		if err != nil {
			t.Error("unexpected error")
		}
		obj.Log().Debug("debug")
		obj.Log().Info("info")
		obj.Log().Warn("warn")
		obj.Log().Error("error")

		logLines = strings.Split(sb.String(), "\n")
		if len(logLines) != 2 {
			t.Error("Unexpected log length", len(logLines), "\n", logLines)
		}
	})

	t.Run("SeparateObjectLogs", func(t *testing.T) {
		obj1 := gonatus.Gobject{}
		sb1 := &strings.Builder{}
		obj1.SetLog(logging.NewTextLogger(sb1, slog.LevelDebug.Level()))

		obj2 := gonatus.Gobject{}
		sb2 := &strings.Builder{}
		obj2.SetLog(logging.NewTextLogger(sb2, slog.LevelError.Level()))

		obj1.Log().Debug("debug")
		obj2.Log().Debug("debug")
		obj1.Log().Info("info")
		obj2.Log().Info("info")
		obj1.Log().Warn("warn")
		obj2.Log().Warn("warn")
		obj1.Log().Error("error")
		obj2.Log().Error("error")

		logLines := strings.Split(sb1.String(), "\n")
		if len(logLines) != 5 {
			t.Error("Unexpected log length", len(logLines), "\n", logLines)
		}
		logLines = strings.Split(sb2.String(), "\n")
		if len(logLines) != 2 {
			t.Error("Unexpected log length", len(logLines), "\n", logLines)
		}
	})

	t.Run("RuntimeDefaultLoggerChange", func(t *testing.T) {
		obj1 := gonatus.Gobject{}

		obj2 := gonatus.Gobject{}
		sb2 := &strings.Builder{}
		log2 := logging.NewTextLogger(sb2, slog.LevelDebug.Level())
		obj2.SetLog(log2)

		sb1 := &strings.Builder{}
		log1 := logging.NewTextLogger(sb1, slog.LevelDebug.Level())
		logging.SetDefaultLogger(log1)

		obj1.Log().Debug("debug")
		obj2.Log().Debug("debug")
		obj1.Log().Info("info")
		logging.SetDefaultLogger(log2)
		obj2.Log().Info("info")
		obj1.Log().Warn("warn")
		obj2.Log().Warn("warn")
		logging.SetDefaultLogger(log1)
		obj1.Log().Error("error")
		obj2.Log().Error("error")

		logLines := strings.Split(sb1.String(), "\n")
		if len(logLines) != 4 {
			t.Error("Unexpected log length", len(logLines), "\n", logLines)
		}
		logLines = strings.Split(sb2.String(), "\n")
		if len(logLines) != 6 {
			t.Error("Unexpected log length", len(logLines), "\n", logLines)
		}
	})

	t.Run("RuntimeObjectLoggerChange", func(t *testing.T) {
		obj := gonatus.Gobject{}
		sb1 := &strings.Builder{}
		obj.SetLog(logging.NewTextLogger(sb1, slog.LevelDebug.Level()))

		obj.Log().Debug("debug")
		obj.Log().Info("info")
		obj.Log().Warn("warn")

		sb2 := &strings.Builder{}
		obj.SetLog(logging.NewTextLogger(sb2, slog.LevelError.Level()))

		obj.Log().Error("error")

		logLines := strings.Split(sb1.String(), "\n")
		if len(logLines) != 4 {
			t.Error("Unexpected log length", len(logLines), "\n", logLines)
		}
		logLines = strings.Split(sb2.String(), "\n")
		if len(logLines) != 2 {
			t.Error("Unexpected log length", len(logLines), "\n", logLines)
		}
	})

}
