package bootstrap

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aririfani/message-service/internal/collector"
	"golang.org/x/sync/errgroup"
)

var (
	brokers      = []string{"localhost:9092"}
	runCollector = flag.Bool("collector", false, "run collector processor")
)

func Run() {
	flag.Parse()
	ctx, cancle := context.WithCancel(context.Background())
	grp, ctx := errgroup.WithContext(ctx)

	if *runCollector {
		collector.PrepareTopics(brokers)
	}

	if *runCollector {
		log.Println("starting collector")
		grp.Go(collector.Run(ctx, brokers))
	}

	// wait for SIGINT/SIGTERM
	waiter := make(chan os.Signal, 1)
	signal.Notify(waiter, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-waiter:
	case <-ctx.Done():
	}

	cancle()
	if err := grp.Wait(); err != nil {
		log.Println(err)
	}
	log.Println("done")
}
