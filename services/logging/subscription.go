package logging

// Subscription provides access to the stream of logs.
type Subscription struct {
	match map[string]interface{}
	buf   []entry
}

func (s *Subscription) Collect(e entry) {
	if !s.matches(e) {
		return
	}
	s.buf = append(s.buf, e)
}

func (s *Subscription) matches(e entry) bool {
	if e.Level < s.Level {
		return false
	}
	for k, v := range s.match {
		if got, ok := e.Fields[k]; !ok || v != got {
			return false
		}
	}
	return true
}
