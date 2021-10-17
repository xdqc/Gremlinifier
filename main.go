package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	gremcos "github.com/supplyon/gremcos"

	q "github.com/xdqc/cosmos-gremlin/query"
)

func main() {
	q.Logger = zerolog.New(os.Stdout).Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.StampMilli}).With().Timestamp().Logger()
	q.Cosmos = connectCosmos(q.Logger)

	defer func() {
		// year := strings.Join(os.Args[1:], " ")
		// q.WikiEventByYear(year)
		// time.Sleep(time.Second * 2)
		// q.QueryCosmos(fmt.Sprintf("g.V('%s').inE().outV()", year))
		// q.QueryCosmos(fmt.Sprintf("g.V('%s').inE().outV().outE().order().by('label')", year))
		// q.QueryCosmos(("g.V().hasLabel('year').order().by('id').range(local,30,-1)"))

		addEdgePatientHasPathology()
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

func addEdgeTumorDiagnose() {
	tumors := q.QueryCosmosValues("g.V().haslabel('primary_tumor').project('properties').by(__.valueMap())")
	for _, t := range tumors {
		tumor := t.Value.(map[string]interface{})["properties"]
		tumor_name := tumor.(map[string]interface{})["name"].([]interface{})[0]
		diagnosis := tumor.(map[string]interface{})["diagnosis"].([]interface{})[0]
		diagnose_dates := tumor.(map[string]interface{})["diagnose_date"]
		intake_dates := tumor.(map[string]interface{})["intake_date"]
		gremlin := fmt.Sprintf(`g
		.V().haslabel('icd10').has('name', '%s').as('diag')
		.V().haslabel('primary_tumor').has('name', '%s').as('tu')
		.coalesce(
			__.outE('diagnose_of'),
			__.addE('diagnose_of').to('diag')
		)`, diagnosis, tumor_name)
		if diagnose_dates != nil {
			gremlin += fmt.Sprintf(".property('diagnose_date', '%s')", diagnose_dates.([]interface{})[0])
		}
		if intake_dates != nil {
			gremlin += fmt.Sprintf(".property('intake_date', '%s')", intake_dates.([]interface{})[0])
		}
		// q.Logger.Debug().Msg(gremlin)
		q.QueryCosmos(gremlin)
	}
}

func addEdgePatientHasPathology() {
	pathologies := q.QueryCosmosValues("g.V().haslabel('pathology_analysis').project('id','properties').by(__.id()).by(__.valueMap())")
	for _, pa := range pathologies {
		paid := pa.Value.(map[string]interface{})["id"]
		pp := pa.Value.(map[string]interface{})["properties"]
		pk := pp.(map[string]interface{})["pk"].([]interface{})[0]
		// paname := pp.(map[string]interface{})["name"].([]interface{})[0]
		sample_date := pp.(map[string]interface{})["sample_date"].([]interface{})[0]
		report_date := pp.(map[string]interface{})["report_date"]
		gremlin := fmt.Sprintf(`g.V().has('id', '%s').as('pa').outE().haslabel('pathology_diagnose_tumor').select('pa')
		.coalesce(
			__.outE('pathology_report_diagnose'),
			__.addE('pathology_report_diagnose').to(__.V().has('id','%s'))
		).property('sample_date', '%s')`, paid, pk, sample_date)
		// gremlin = fmt.Sprintf(`g.V().has('id', '%s').as('pa')
		// .coalesce(
		// 	 __.inE('has_pathology'),
		// 	__.addE('has_pathology').from(__.V().has('id','%s'))
		// ).property('sample_date', '%s')`, paid, pk, sample_date)
		if report_date != nil {
			gremlin += fmt.Sprintf(".property('report_date', '%s')", report_date.([]interface{})[0])
		}
		// q.Logger.Debug().Msgf(gremlin)
		q.QueryCosmos(gremlin)
	}
}

func addEdgePathologyFollow(pType string) {
	patients := q.QueryCosmosValues("g.V().haslabel('patient').properties('pk').value()")
	for _, p := range patients {
		pk := fmt.Sprintf("%s", p.Value)
		gremlin := fmt.Sprintf("g.V().has('pk', '%s').haslabel('pathology_analysis').has('name', TextP.startingWith('%s')).order().by('sample_date').properties('sample_date').value()",
			pk, pType)
		pathos_dates := q.QueryCosmosValues(gremlin)
		prev_date := ""
		for i, p := range pathos_dates {
			sample_date := fmt.Sprintf("%s", p.Value)
			if prev_date != "" && prev_date != sample_date {
				gremlin = fmt.Sprintf(`g
				.V().has('pk', '%s').has('name', TextP.startingWith('%s')).has('sample_date','%s').as('op')
				.V().has('pk', '%s').has('name', TextP.startingWith('%s')).has('sample_date','%s').as('np')
				.coalesce(
					__.outE('follow_pathology'),
					__.addE('follow_pathology').to('op')
				)`, pk, pType, prev_date, pk, pType, sample_date)
				q.Logger.Info().Msgf("%s %d %v", pk, i, sample_date)
				q.QueryCosmos(gremlin)
			}
			prev_date = sample_date
		}
	}
}

func readICD10() map[string]string {
	icd10desc := make(map[string]string)

	f, err := os.Open("data/icd10cm_order_2019.tsv")
	if err != nil {
		q.Logger.Error().Err(err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			q.Logger.Error().Err(err)
		}
		rec = strings.Split(rec[0], "\t")
		icdkey := strings.TrimSpace(rec[1])
		icdkey = icdkey[:3] + "." + icdkey[3:]
		icd10desc[icdkey] = rec[3]
	}
	return icd10desc
}

func connectCosmos(logger zerolog.Logger) gremcos.Cosmos {
	host := os.Getenv("CDB_HOST")
	if len(host) == 0 {
		logger.Fatal().Msg("Host not set. Use export CDB_HOST=<CosmosDB Gremlin Endpoint> to specify it")
	}
	logger.Debug().Msg(host)

	credProvider := q.DynamicCredentialProvider{CredentialFile: "../cosmos_dynamic_credentials/credentials.json"}
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
