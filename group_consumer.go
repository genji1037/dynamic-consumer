package dcon

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

type ConsumerGroup struct {
	consumerFactory NewConsumer
	adjustInterval  time.Duration // adjust consumers nums every interval.
	workChan        chan Work
	//TotalWorkNum      uint64
	consumedWorkNum uint64

	consumers         []Consumer
	consumerCancelers []func()
}

type ConsumerGroupOption struct {
	ConsumerFactory NewConsumer
	WorkChan        chan Work
	AdjustInterval  time.Duration // adjust consumers nums every interval.
	OriConsumerNum  int
}

func (opt *ConsumerGroupOption) init() {
	if opt.OriConsumerNum == 0 {
		opt.OriConsumerNum = 4
	}
	if opt.AdjustInterval < 10*time.Second {
		opt.AdjustInterval = 10 * time.Second
	}
}

// GroupConsume new a consumer group with consumer's factory method and work channel and specified consumer nums.
// then start consume.
func GroupConsume(opt ConsumerGroupOption) *ConsumerGroup {
	cg := ConsumerGroup{
		consumers:       make([]Consumer, 0, opt.OriConsumerNum),
		adjustInterval:  opt.AdjustInterval,
		workChan:        opt.WorkChan,
		consumerFactory: opt.ConsumerFactory,
	}
	cg.addConsumer(opt.OriConsumerNum)
	go cg.backgroundAdjust()
	return &cg
}

func (cg *ConsumerGroup) ConsumerNum() int {
	return len(cg.consumers)
}

func (cg *ConsumerGroup) backgroundAdjust() {
	for {
		oriWorkDepth := len(cg.workChan)
		oriConsumed := cg.consumedWorkNum
		time.Sleep(cg.adjustInterval)
		workDepth := len(cg.workChan)
		consumedIncr := cg.consumedWorkNum - oriConsumed
		fmt.Println("consumedIncr", consumedIncr)

		if workDepth >= oriWorkDepth && oriWorkDepth > 0 {
			cg.addConsumer(1)
		}
		if workDepth == 0 {
			seconds := cg.adjustInterval / time.Second
			currentTPS := int(consumedIncr) / int(seconds)
			if len(cg.consumers) > 4 && currentTPS*2 < len(cg.consumers) {
				cg.removeConsumer(1)
			}
		}

	}
}

func (cg *ConsumerGroup) addConsumer(num int) {
	for i := 0; i < num; i++ {
		consumer := cg.consumerFactory()
		ctx, cancel := context.WithCancel(context.Background())
		go cg.startConsume(ctx, consumer)
		cg.consumers = append(cg.consumers, consumer)
		cg.consumerCancelers = append(cg.consumerCancelers, cancel)
	}
}

func (cg *ConsumerGroup) removeConsumer(num int) {
	l := len(cg.consumers)
	if l != len(cg.consumerCancelers) {
		fmt.Printf("removeConsumer failed: len(cg.consumers) %d != len(cg.consumerCancelers) %d \n",
			len(cg.consumers), len(cg.consumerCancelers))
		return
	}
	if num >= l {
		fmt.Printf("removeConsumer failed: remove num %d >= len(cg.consumers) %d \n", num, len(cg.consumers))
		return
	}
	for n := 0; n < num; n++ {
		i := l - 1 - n
		cg.consumerCancelers[i]()
	}
	cg.consumerCancelers = cg.consumerCancelers[:l-num]
	cg.consumers = cg.consumers[:l-num]
}

func (cg *ConsumerGroup) startConsume(ctx context.Context, consumer Consumer) {
	for {
		select {
		case work := <-cg.workChan:
			consumer.Consume(work)
			atomic.AddUint64(&cg.consumedWorkNum, 1)
		case <-ctx.Done():
			return
		}
	}
}
