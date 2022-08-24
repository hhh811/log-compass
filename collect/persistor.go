package collect

import (
	"github.com/hhh811/log-compass/msg"
)

type Persistor interface {
	Persist([]msg.Message) error // a time consuming process
}
