package tick_test

import (
	"fmt"
	"testing"
)

func TestTelegrafOut(t *testing.T) {
	pipe, _, from := StreamFrom()
	telegrafOut := from.TelegrafOut()
	_ = pipe
	_ = telegrafOut
	fmt.Println(PipelineTick(pipe))
}
