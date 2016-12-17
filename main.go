package main

import (
	"log"
	"math"
	"time"
)

type InputMessage struct {
}

type PreProcessedMessage struct {
}

type Sourcer interface {
	Messages() []InputMessage
}

type SteadySourcer struct {
	last time.Time
	rate float64
}

// NewSteadySourcer creates a SteadySourcer which produces [rate] messages per
// second
func NewSteadySourcer(rate float64) *SteadySourcer {
	return &SteadySourcer{
		last: time.Now(),
		rate: rate,
	}
}

func (s *SteadySourcer) Messages() []InputMessage {
	now := time.Now()

	since := now.Sub(s.last).Seconds()

	ideal_count := since * s.rate

	real_count := math.Floor(ideal_count)

	extra_nanoseconds := time.Duration(((ideal_count - real_count) / s.rate) * -1e9)

	s.last = now.Add(extra_nanoseconds)

	count := int(real_count)

	ret := make([]InputMessage, count)
	for n := 0; n < count; n++ {
		ret[n] = InputMessage{}
	}

	return ret
}

func main() {
	// start a pipeline for processing messages
	pipelineChan := make(chan InputMessage)
	go PipeLine(pipelineChan, 10, 40, 5*time.Second)

	// make a source
	source := NewSteadySourcer(1000.0)

	// go on forever
	i := -1
	for {
		i++
		// get all of the available messages at this time
		messages := source.Messages()
		c := len(messages)
		if c > 0 {
			log.Printf("got %d messages after %d loops of silence\n", c, i)
			i = 0
		}

		// shove each message into the unbuffered pipeline as quickly as possible
		for _, message := range messages {
			s := time.Now()
			pipelineChan <- message
			log.Printf("message inserted into pipeline in %s\n", time.Now().Sub(s).String())
		}
	}
}

func NoopPreProcessor(i InputMessage) PreProcessedMessage {
	return PreProcessedMessage{}
}

func PipeLine(messages <-chan InputMessage, buffer, batcherTarget int, batcherInterval time.Duration) {
	// start a batcher to bundle the output of preprocessed messages
	batcherChan := make(chan PreProcessedMessage, buffer)
	go Batcher(batcherChan, batcherTarget, batcherInterval)

	// process the messages and hand them off to the batcher as quickly as possible
	for {
		select {
		case m := <-messages:
			s := time.Now()
			p := NoopPreProcessor(m)
			batcherChan <- p
			log.Printf("message sent to batcher in %s\n", time.Now().Sub(s).String())
		}
	}
}

func DoTheOutput(messages []PreProcessedMessage, count int) {
	log.Printf("outputting %d message(s)", count)
}

func Batcher(messages <-chan PreProcessedMessage, target int, interval time.Duration) {
	// create our first output batch
	batch := make([]PreProcessedMessage, target)
	i := 0
	timer := time.NewTimer(interval)

	for {
		select {
		case m := <-messages:
			batch[i] = m
			i++
			if i >= target {
				DoTheOutput(batch, i)
				batch = make([]PreProcessedMessage, target)
				i = 0
				timer.Reset(interval)
			}

		case <-timer.C:
			DoTheOutput(batch, i)
			batch = make([]PreProcessedMessage, target)
			i = 0
			timer.Reset(interval)
		}
	}
}
