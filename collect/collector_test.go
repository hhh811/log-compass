package collect

import (
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hhh811/log-compass/log"
	"github.com/hhh811/log-compass/msg"
)

type moc struct {
	sent         int64
	recv         int64
	persistDelay int
	sendDelay    int
}

func (m *moc) Consume(msgs []msg.Message) error {
	if m.persistDelay > 0 {
		time.Sleep(time.Duration(m.persistDelay) * time.Millisecond)
	}
	for _, msg := range msgs {
		if v, e := strconv.ParseInt(msg.Content, 10, 32); e != nil {
			return e
		} else {
			atomic.AddInt64(&m.recv, v)
		}
	}
	return nil
}

func (m *moc) ConsumeSingle(ms msg.Message) error {
	return m.Consume([]msg.Message{ms})
}

func sendLoop(t *testing.T, m *moc, c *CollectorImp, loop int, sig chan struct{}) {
	for i := 0; i < loop; i++ {
		rand.Seed(time.Now().UnixNano())
		num := rand.Int63n(100)
		if m.sendDelay > 0 && rand.Float32() < 0.1 {
			time.Sleep(time.Duration(m.sendDelay) * time.Millisecond)
		}
		item := msg.Message{
			CollectTime: time.Now(),
			Label:       "test",
			Creator:     strconv.FormatInt(int64(i), 10),
			Content:     strconv.FormatInt(num, 10),
		}
		c.Collect(item)
		// t.Logf("sent %s %s\n", item.Creator, item.Content)
		m.sent += num
	}
	sig <- struct{}{}
}

func TestStart_Simple(t *testing.T) {
	log.Setup(log.LogConf{
		Level: "info",
	})

	m := &moc{
		persistDelay: 50,
		sendDelay:    50,
	}

	cf := CollectorConf{
		triggerCacheSize:    100,
		triggerMilliSeconds: 50,
	}

	c := NewCollector(cf, m)

	c.Start()

	sendStopSig := make(chan struct{})
	go sendLoop(t, m, c, 100000, sendStopSig)

	<-sendStopSig
	t.Log("send end\n")

	time.Sleep(time.Duration(2) * time.Second)

	c.Stop()
	r := atomic.LoadInt64(&m.recv)
	t.Logf("sent %d, received %d\n", m.sent, r)
	if m.sent != r {
		t.Errorf("sent %d not equal to received %d\n", m.sent, r)
	}
	time.Sleep(time.Duration(1) * time.Second)
}
