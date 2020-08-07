package dcon

import (
	"time"
)

type mockConsumer struct {
	Latency time.Duration
}

func NewMockConsumer() Consumer {
	return mockConsumer{
		Latency: 500 * time.Millisecond,
	}
}

func (c mockConsumer) Consume(work Work) {
	time.Sleep(c.Latency)
	//fmt.Println("consumed work:", work.(int64))
}
