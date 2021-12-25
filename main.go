package main

import (
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	gremcos "github.com/supplyon/gremcos"

	q "github.com/xdqc/gremlinifier/query"
)

func main() {
	q.Init(true)

	exitChannel := make(chan interface{})
	go processLoop(q.Cosmos, q.Logger, exitChannel)
	<-exitChannel

	if err := q.Cosmos.Stop(); err != nil {
		q.Logger.Error().Err(err).Msg("Failed to stop cosmos connector")
	}
	q.Logger.Info().Msg("Teared down")
}

func processLoop(cosmos gremcos.Cosmos, logger zerolog.Logger, exitChan chan<- interface{}) {
	// register for common exit signals (e.g. ctrl-c)
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	// create tickers for doing health check and queries
	queryTicker := time.NewTicker(time.Second * 2)
	healthCheckTicker := time.NewTicker(time.Second * 30)

	// ensure to clean up as soon as the processLoop has been left
	defer func() {
		queryTicker.Stop()
		healthCheckTicker.Stop()
	}()

	//List existing years
	q.QueryCosmos(("g.V().hasLabel('year').order().by('id').range(local,30,-1)"))
	year := readArgsAsInt()

	stopProcessing := false
	logger.Info().Msg("Process loop entered")
	for !stopProcessing {
		select {
		case <-signalChannel:
			exitChan <- signalChannel
			stopProcessing = true
		case <-queryTicker.C:
			q.WikiEventServiceByYear(year)
			year = year + 1
		case <-healthCheckTicker.C:
			err := cosmos.IsHealthy()
			logEvent := logger.Debug()
			if err != nil {
				logEvent = logger.Warn().Err(err)
			}
			logEvent.Bool("healthy", err == nil).Msg("Health Check")
		}
	}

	logger.Info().Msg("Process loop left")
}

func readArgsAsInt() int {
	year, err := strconv.Atoi(strings.Join(os.Args[1:], " "))
	if err != nil {
		q.Logger.Panic().Msgf(err.Error())
	}
	return year
}
