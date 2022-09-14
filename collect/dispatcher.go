package collect

import "github.com/hhh811/log-compass/msg"

// dispatch message
type Dispatcher interface {
	Consumer
	RegisterConsumer(c Consumer, label string) error
}

type DispatcherImpl struct {
	consumerMap     map[string]Consumer
	defaultConsumer Consumer
}

func New() *DispatcherImpl {
	return &DispatcherImpl{
		consumerMap:     make(map[string]Consumer),
		defaultConsumer: Discard(),
	}
}

func (d *DispatcherImpl) RegisterConsumer(c Consumer, label string) error {
	d.consumerMap[label] = c
	return nil
}

func (d *DispatcherImpl) Consume(msgs []msg.Message) error {
	for _, m := range msgs {
		if c, ok := d.consumerMap[m.Label]; ok {
			c.ConsumeSingle(m)
		}
	}

	// handle error
	return nil
}
