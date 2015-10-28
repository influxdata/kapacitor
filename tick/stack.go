package tick

import (
	"bytes"
	"errors"
	"fmt"
	//"log"
)

var ErrEmptyStack = errors.New("stack is empty")

type stack struct {
	data []interface{}
}

func (s *stack) Len() int {
	return len(s.data)
}

func (s *stack) Push(v interface{}) {
	s.data = append(s.data, v)
	//log.Println("push", s)
}

func (s *stack) Pop() interface{} {
	if s.Len() > 0 {
		l := s.Len() - 1
		v := s.data[l]
		s.data = s.data[:l]
		//log.Println("pop", s)
		return v
	}
	panic(ErrEmptyStack)
}

func (s *stack) String() string {
	var str bytes.Buffer
	str.Write([]byte("s["))
	for i := len(s.data) - 1; i >= 0; i-- {
		fmt.Fprintf(&str, "%T:%v,", s.data[i], s.data[i])
	}
	str.Write([]byte("]"))
	return str.String()
}
