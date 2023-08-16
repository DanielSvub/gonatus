package gonatus

type Service interface {
	Gobjecter
	New(Conf) (GId, error)
	Remove(ids ...GId) error
}

type DefaultService[T Gobjecter] struct {
	service     Service
	objects     map[GId]T
	constructor func(Conf) (T, error)
}

func NewDefaultService[T Gobjecter](service Service, constructor func(Conf) (T, error)) *DefaultService[T] {
	ego := &DefaultService[T]{
		service:     service,
		objects:     make(map[GId]T),
		constructor: constructor,
	}
	GKeeper.Register(ego.service)
	return ego
}

func (ego *DefaultService[T]) New(conf Conf) (GId, error) {
	if object, err := ego.constructor(conf); err != nil {
		return 0, err
	} else {
		id := GKeeper.NewId()
		object.SetId(id)
		ego.objects[id] = object
		return id, nil
	}
}

func (ego *DefaultService[T]) Remove(ids ...GId) error {
	for _, id := range ids {
		ego.objects[id].SetId(0)
		delete(ego.objects, id)
	}
	return nil
}

func (ego *DefaultService[T]) Fetch(id GId) Gobjecter {
	return ego.objects[id]
}

/*
A service which keeps track of the services.

Extends:
  - gonatus.Gobject.

Implements:
  - gonatus.Gobjecter.
*/
type GonatusKeeper struct {
	Gobject
	counter  GId
	services map[GId]Service
}

// Default storage manager
var GKeeper GonatusKeeper = GonatusKeeper{services: make(map[GId]Service)}

/*
Registers a new storage to the manager.

Parameters:
  - s - storage to register.
*/
func (ego *GonatusKeeper) Register(s Service) {
	ego.counter++
	ego.services[ego.counter] = s
	s.SetId(ego.counter)
}

/*
Unregisters a storage from the manager.

Parameters:
  - s - storage to unregister.

Returns:
  - error if any occurred.
*/
func (ego *GonatusKeeper) Unregister(s Service) error {
	delete(ego.services, s.Id())
	s.SetId(0)
	return nil
}

/*
Fetches a storage with the given ID.

Parameters:
  - e - ID of the storage.

Returns:
  - the storage (nil if not found),
  - error if not found.
*/
func (ego *GonatusKeeper) Fetch(id GId) Service {
	return ego.services[id]
}

func (ego *GonatusKeeper) NewId() GId {
	ego.counter++
	return ego.counter
}
