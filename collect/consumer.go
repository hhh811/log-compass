package collect

import (
	"github.com/hhh811/log-compass/msg"
)

type Consumer interface {
	Consume([]msg.Message) error // a time consuming process
	ConsumeSingle(msg.Message) error
}

type discard struct{}

func (d *discard) Consume([]msg.Message) error {
	// do nothing
	return nil
}

func (d *discard) ConsumeSingle(msg.Message) error {
	// do nothing
	return nil
}

var discard0 = &discard{}

func Discard() Consumer {
	return discard0
}
