package dcon

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"testing"
	"time"
)

func TestGroupConsume(t *testing.T) {
	go http.ListenAndServe("localhost:18080", http.DefaultServeMux)

	workChan := make(chan Work, 1024)
	option := ConsumerGroupOption{
		ConsumerFactory: NewMockConsumer,
		WorkChan:        workChan,
		AdjustInterval:  5 * time.Second,
		OriConsumerNum:  8,
	}
	cg := GroupConsume(option)

	go observe(workChan, cg)

	produceInterval := 50 * time.Millisecond
	go produce(workChan, &produceInterval)

	go produceSpeedControl(&produceInterval)

	select {}

}

func observe(c chan Work, cg *ConsumerGroup) {
	for {
		time.Sleep(time.Second)
		fmt.Println("work depth:", len(c), "consumer num:", cg.ConsumerNum())
	}
}

func produce(c chan Work, interval *time.Duration) {
	for {
		time.Sleep(*interval)
		work := time.Now().Unix()
		select {
		case c <- work:
		default:
		}
	}
}

func produceSpeedControl(interval *time.Duration) {
	for {
		time.Sleep(40 * time.Second)
		fmt.Println("speed up")
		*interval = *interval / 2

		time.Sleep(40 * time.Second)
		fmt.Println("speed down")
		*interval = *interval * 20

		time.Sleep(40 * time.Second)
		fmt.Println("speed up")
		*interval = *interval / 20

		time.Sleep(40 * time.Second)
		fmt.Println("speed done")
		*interval = *interval * 2
	}
}
