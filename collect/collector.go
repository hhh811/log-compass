package collect

import (
	"time"

	"github.com/hhh811/log-compass/log"
	"github.com/hhh811/log-compass/msg"
)

type CollectorImp struct {
	conf CollectorConf

	cache []msg.Message // cache of rec

	logCh chan msg.Message

	persistQueue chan []msg.Message

	stopSig chan struct{} // sig to stop

	loopEndSig chan struct{}

	ticker time.Ticker

	persistor Persistor
}

func NewCollector(conf CollectorConf, persistor Persistor) *CollectorImp {
	return &CollectorImp{
		conf:         conf,
		logCh:        make(chan msg.Message),
		persistQueue: make(chan []msg.Message, 1),
		stopSig:      make(chan struct{}),
		loopEndSig:   make(chan struct{}),
		ticker:       *time.NewTicker(time.Millisecond * time.Duration(conf.triggerMilliSeconds)),
		persistor:    persistor,
	}
}

func (c *CollectorImp) Collect(msg msg.Message) {
	c.logCh <- msg
}

func (c *CollectorImp) Start() {
	go c.collectLoop()
	go c.persistLoop()
	log.Infof("start!")
}

func (c *CollectorImp) Stop() {
	close(c.stopSig)
	close(c.logCh)
	c.finish()
	log.Infof("exit bye!")
}

func (c *CollectorImp) collectLoop() {
	for {
		select {
		case item := <-c.logCh:
			log.Debugf("receive msg %s %s", item.Creator, item.Content)
			c.cache = append(c.cache, item)
			if len(c.cache) > c.conf.triggerCacheSize {
				log.Debugf("cache size reach %d, trigger persist", c.conf.triggerCacheSize)
				c.tryPersist()
			}
		case <-c.ticker.C:
			log.Debugf("ticker trigger persist")
			c.tryPersist()
		case <-c.stopSig:
			close(c.loopEndSig)
			return
		}
	}
}

// this function must not block
func (c *CollectorImp) tryPersist() {
	select {
	case c.persistQueue <- c.cache[:]:
		c.cache = []msg.Message{}
	default:
	}
}

func (c *CollectorImp) persistLoop() {
	for {
		select {
		case <-c.stopSig:
			return
		case logs := <-c.persistQueue:
			if len(logs) == 0 {
				continue
			}
			if e := c.persistor.Persist(logs); e != nil {
				// todo how to deal with persist error
				log.Errorf("persist error %q", e)
			}
			log.Debugf("persist %d logs %s to %s success", len(logs), logs[0].Creator, logs[len(logs)-1].Creator)
		}
	}
}

func (c *CollectorImp) finish() {
	select {
	case logs := <-c.persistQueue:
		if e := c.persistor.Persist(logs); e != nil {
			// todo how to deal with persist error
			log.Errorf("persist error %q", e)
		}
		log.Debugf("persist %d logs %s to %s success", len(logs), logs[0].Creator, logs[len(logs)-1].Creator)
	default:
	}
	<-c.loopEndSig
	c.persistor.Persist(c.cache)
}
