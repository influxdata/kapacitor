package alert

type Service struct {
	topics map[string]Topic
	// Map topic name -> []Handler
	handlers map[string][]Handler
}

func (s *Service) Collect(event Event) {

}

func (s *Service) RegisterHandler(topics []string, h Handler) {
}

func (s *Service) DeregisterHandler(topics []string, h Handler) {
}

func (s *Service) TopicsStatus(pattern string, minLevel Level) map[string]Level {
	panic("unimplemented")
}

func (s *Service) TopicsStatusDetails(pattern string, minLevel Level) map[string]map[string]EventState {
	panic("unimplemented")
}

type Topic struct {
	Name   string
	Events map[string]EventState
}

func (t *Topic) MaxLevel() Level {
	panic("unimplemented")
}

func (t *Topic) UpdateEvent(id string, state EventState) {
}
