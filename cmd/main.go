package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	gremcos "github.com/supplyon/gremcos"
	histo "github.com/xdqc/cosmos-gremlin"
	q "github.com/xdqc/cosmos-gremlin/query"
)

func main() {
	q.Logger = zerolog.New(os.Stdout).Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.StampMilli}).With().Timestamp().Logger()
	q.Cosmos = connectCosmos(q.Logger)
	defer func() {
		year := strings.Join(os.Args[1:], " ")
		q.WikiEventByYear(year)
		time.Sleep(time.Second * 2)
		q.QueryCosmos(fmt.Sprintf("g.V('%s').inE().outV()", year))
		q.QueryCosmos(fmt.Sprintf("g.V('%s').inE().outV().outE().order().by('label')", year))
		q.QueryCosmos(fmt.Sprintf("g.E().has('by_year', '%s')", year))
	}()
	// cosmos := connectCosmos(logger)
	// query.QueryCosmos(cosmos, logger)
	// exitChannel := make(chan interface{})
	// // go processLoop(cosmos, logger, exitChannel)
	// <-exitChannel

	// if err := cosmos.Stop(); err != nil {
	// 	logger.Error().Err(err).Msg("Failed to stop cosmos connector")
	// }
	// logger.Info().Msg("Teared down")
}

func connectCosmos(logger zerolog.Logger) gremcos.Cosmos {
	host := os.Getenv("CDB_HOST")
	if len(host) == 0 {
		logger.Fatal().Msg("Host not set. Use export CDB_HOST=<CosmosDB Gremlin Endpoint> to specify it")
	}
	logger.Debug().Msg(host)

	credProvider := histo.DynamicCredentialProvider{CredentialFile: "../cosmos_dynamic_credentials/credentials.json"}
	cosmos, err := gremcos.New(host,
		gremcos.WithResourceTokenAuth(&credProvider),
		// gremcos.WithLogger(logger),
		gremcos.NumMaxActiveConnections(10),
		gremcos.ConnectionIdleTimeout(time.Second*30),
		gremcos.MetricsPrefix("myservice"),
	)

	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create the cosmos connector")
	}
	return cosmos
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

	stopProcessing := false
	logger.Info().Msg("Process loop entered")
	for !stopProcessing {
		select {
		case <-signalChannel:
			exitChan <- signalChannel
			stopProcessing = true
		case <-queryTicker.C:
			// q.QueryCosmos()
			// queryCosmosWithBindings(cosmos, logger)
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
