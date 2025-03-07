package main

import (
	crand "crypto/rand"
	"flag"
	"log"
	"log/slog"
	mrand "math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	var defaultTopic string
	var brokers string
	var recordTries int
	var metadataMaxAge string
	var workerPeriod string
	var workerCount int

	flag.StringVar(&defaultTopic, "topic", "test-topic", "default topic to publish to")
	flag.StringVar(&brokers, "brokers", "localhost:19092,localhost:29092,localhost:39092", "comma separated list of brokers")
	flag.IntVar(&recordTries, "tries", 1, "record max tries (0 for unlimited)")
	flag.StringVar(&metadataMaxAge, "metadata-max-age", "5m", "metadata max age")
	flag.IntVar(&workerCount, "worker-count", 10, "worker count")
	flag.StringVar(&workerPeriod, "worker-period", "1s", "worker period, how often each worker produces message batches")
	flag.Parse()

	periodDuration, err := time.ParseDuration(workerPeriod)
	if err != nil {
		panic(err)
	}

	metaMaxAgeDuration, err := time.ParseDuration(metadataMaxAge)
	if err != nil {
		panic(err)
	}

	l := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(l)

	publisher, err := newFranzProducer(
		strings.Split(brokers, ","),
		defaultTopic,
		recordTries,
		metaMaxAgeDuration,
	)
	if err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}
	stopCh := makeSignalChannel()

	for i := range workerCount {
		wg.Add(1)
		go produce(i, wg, stopCh, periodDuration, publisher)
	}

	wg.Wait()
}

func produce(id int, wg *sync.WaitGroup, stopCh <-chan struct{}, interval time.Duration, p *FranzProducer) {
	defer wg.Done()
	timer := time.NewTicker(interval)
	delay := time.Duration(mrand.Int63n(interval.Milliseconds())) * time.Millisecond
	time.Sleep(delay)

	log.Printf("worker %d started after %s", id, delay)

	for {
		select {
		case <-stopCh:
			return
		case <-timer.C:
			batch := make([][]byte, 0)
			for range mrand.Int31n(512) + 1 {
				buf := make([]byte, mrand.Int()%256+128)
				_, _ = crand.Read(buf)
				batch = append(batch, buf)
			}

			results := p.ProduceBatch(batch)
			for i, result := range results {
				if result.Err != nil {
					log.Printf("failed to publish event %d: %v", i, result.Err)
				}
			}
		}
	}
}

func makeSignalChannel() <-chan struct{} {
	stopCh := make(chan struct{})
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
		s := <-c
		log.Printf("received signal %s, stopping", s)
		close(stopCh)
	}()

	return stopCh
}
