package gonatus

type Conf any

type Gobjecter interface {
	Serialize() Conf
}

type Gobject struct{}
