package gonatus_test

import (
	"strings"
	"testing"
	"time"

	. "github.com/SpongeData-cz/gonatus"
	"golang.org/x/exp/slog"
)

type Dog struct {
	Gobject
	Name string
	Age  int
}

func NewDog(conf Conf) *Dog {
	ego := new(Dog)
	conf.Load(ego)
	ego.Init(ego)
	return ego
}

func (ego *Dog) Barf(i int) {
	switch i {
	case 1:
		ego.Log().Info("Woof!")
		return
	case 2:
		ego.Log().Debug("Meow!", slog.Duration("Duration", 10*time.Second))
		return
	default:
		ego.Log().Error("Quack!")
		return
	}
}

func TestGonatusBase(t *testing.T) {
	init := NewConf("Dog")
	init["Name"] = "Doge"
	init["Age"] = 2
	dog := NewDog(init)

	conf := dog.Serialize()
	copy := conf.Clone()

	if dog.Ptr() != dog {
		t.Errorf("Object and its pointer are not equal.")
	}

	if conf["Name"] != copy["Name"] {
		t.Errorf("Names are not equal.")
	}

	if conf["Age"] != copy["Age"] {
		t.Errorf("Ages are not equal.")
	}

	test := NewConf("Dog")
	if test.Class() != "Dog" {
		t.Errorf("Invalid class.")
	}

	err := test.Unmarshal([]byte(conf.String()))
	if err != nil {
		t.Errorf("Error during unmarshalling.")
	}

	empty := NewDog(nil)
	if empty.Serialize().Class() != "Dog" {
		t.Errorf("Problem with an empty Conf.")
	}

}

// TestGonatusLogging is very naive test of basic logging capabilities
func TestGonatusLogging(t *testing.T) {
	sb := &strings.Builder{}
	logger := slog.New(slog.NewJSONHandler(sb, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	init := NewConf("Dog")
	init["Name"] = "Doge"
	init["Age"] = 2
	dog := NewDog(init)
	//Logger uninitialized - these messages should be dropped
	dog.Barf(1)
	dog.Barf(2)
	dog.Barf(3)

	logger = logger.With(slog.Group("Dog", "Name", init["Name"], "Age", init["Age"]))
	dog.SetLog(logger)

	// these messages should be logged
	dog.Barf(1)
	dog.Barf(2)
	dog.Barf(3)

	dog.SetLog(nil)

	//Logger uninitialized - these messages should be dropped
	dog.Barf(1)
	dog.Barf(2)
	dog.Barf(3)

	resultLog := sb.String()
	lines := strings.Split(strings.Trim(resultLog, "\n"), "\n")
	if len(lines) != 3 {
		t.Error("Unexpeted number of log lines")
	}
	if !strings.Contains(resultLog, "INFO") || !strings.Contains(resultLog, "DEBUG") || !strings.Contains(resultLog, "ERROR") {
		t.Error("Log levels are not as expected.")
	}

}
