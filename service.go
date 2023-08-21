package gonatus

type RemoteObject interface {
	Id() GId
	SetId(id GId)
}

type Service interface {
	RemoteObject
	New(Conf) (GId, error)
	Remove(ids ...GId) error
}

type DefaultService[T RemoteObject] struct {
	service     Service
	objects     map[GId]T
	constructor func(Conf) (T, error)
}

func NewDefaultService[T RemoteObject](service Service, constructor func(Conf) (T, error)) *DefaultService[T] {
	ego := &DefaultService[T]{
		service:     service,
		objects:     make(map[GId]T),
		constructor: constructor,
	}
	// GKeeper.Register(ego.service)
	return ego
}

func (ego *DefaultService[T]) New(conf Conf) (GId, error) {
	if object, err := ego.constructor(conf); err != nil {
		return 0, err
	} else {
		var id GId
		// id = GKeeper.NewId()
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

func (ego *DefaultService[T]) Fetch(id GId) RemoteObject {
	return ego.objects[id]
}

type GonatusKeeper struct {
	Gobject
	counter  GId
	services map[GId]Service
}

func (ego *GonatusKeeper) Register(s Service) {
	ego.counter++
	ego.services[ego.counter] = s
	s.SetId(ego.counter)
}

func (ego *GonatusKeeper) Unregister(s Service) error {
	delete(ego.services, s.Id())
	s.SetId(0)
	return nil
}

func (ego *GonatusKeeper) Fetch(id GId) Service {
	return ego.services[id]
}

func (ego *GonatusKeeper) NewId() GId {
	ego.counter++
	return ego.counter
}
