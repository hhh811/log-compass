package collect

import (
	"time"

	"github.com/hhh811/log-compass/log"
	"github.com/hhh811/log-compass/msg"
)

type Collector interface {
	Start()
	Stop()
	Collect(msg.Message)
}

type CollectorImp struct {
	conf CollectorConf

	cache []msg.Message // cache of rec

	logCh chan msg.Message

	persistQueue chan []msg.Message

	stopSig chan struct{} // sig to stop

	collectLoopEnd chan struct{}
	consumeLoopEnd chan struct{}

	ticker time.Ticker

	consumer Consumer
}

func NewCollector(conf CollectorConf, collector Consumer) *CollectorImp {
	return &CollectorImp{
		conf:           conf,
		logCh:          make(chan msg.Message),
		persistQueue:   make(chan []msg.Message, 1),
		stopSig:        make(chan struct{}),
		collectLoopEnd: make(chan struct{}),
		consumeLoopEnd: make(chan struct{}),
		ticker:         *time.NewTicker(time.Millisecond * time.Duration(conf.triggerMilliSeconds)),
		consumer:       collector,
	}
}

func (c *CollectorImp) Collect(msg msg.Message) {
	c.logCh <- msg
}

func (c *CollectorImp) Start() {
	go c.collectLoop()
	go c.consumeLoop()
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
			close(c.collectLoopEnd)
			return
		}
	}
}

// this function must not block
func (c *CollectorImp) tryPersist() {
	select {
	case c.persistQueue <- c.cache[:len(c.cache)]:
		c.cache = []msg.Message{}
	default:
	}
}

func (c *CollectorImp) consumeLoop() {
	for {
		select {
		case <-c.stopSig:
			close(c.consumeLoopEnd)
			return
		case logs := <-c.persistQueue:
			if len(logs) == 0 {
				continue
			}
			if e := c.consumer.Consume(logs); e != nil {
				// todo how to deal with persist error
				log.Errorf("persist error %q", e)
			}
			log.Debugf("persist %d logs %s to %s success", len(logs), logs[0].Creator, logs[len(logs)-1].Creator)
		}
	}
}

func (c *CollectorImp) finish() {
	log.Debugf("finish\n")
	<-c.collectLoopEnd
	<-c.consumeLoopEnd
	select {
	case logs := <-c.persistQueue:
		if len(logs) == 0 {
			break
		}
		if e := c.consumer.Consume(logs); e != nil {
			// todo how to deal with persist error
			log.Errorf("persist error %q", e)
		}
		log.Debugf("persist %d logs %s to %s success", len(logs), logs[0].Creator, logs[len(logs)-1].Creator)
	default:
	}
	c.consumer.Consume(c.cache)
}
