package dcon

type Work interface{}

type Consumer interface {
	Consume(work Work)
}

// NewConsumer is consumer's factory method.
type NewConsumer func() Consumer
