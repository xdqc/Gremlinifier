package query

import (
	"os"
	"time"

	"github.com/mattn/go-colorable"
	"github.com/rs/zerolog"
	gremcos "github.com/supplyon/gremcos"
)

func Init(rw bool) {
	Logger = zerolog.New(os.Stdout).Output(zerolog.ConsoleWriter{Out: colorable.NewColorableStdout(), TimeFormat: time.Stamp}).With().Timestamp().Logger()
	Cosmos = connectCosmos(rw, Logger)
}

func connectCosmos(rw bool, logger zerolog.Logger) gremcos.Cosmos {
	host := os.Getenv("CDB_HOST")
	username := os.Getenv("CDB_USERNAME")
	var password string
	if rw {
		password = os.Getenv("CDB_KEY")
	} else {
		password = os.Getenv("CDB_KEY_READONLY")
	}

	credProvider := DynamicCredentialProvider{CredentialFile: "../cosmos_dynamic_credentials/credentials.json"}

	if len(host) == 0 {
		logger.Fatal().Msg("Host not set. Use export CDB_HOST=<CosmosDB Gremlin Endpoint> to specify it")
	}

	logger.Debug().Msg("Connecting using:")
	logger.Debug().Msgf("\thost: %s", host)
	logger.Debug().Msgf("\tusername: %s", username)
	logger.Debug().Msgf("\tpassword is set: %v", len(password) > 0)

	cosmos, err := gremcos.New(host,
		gremcos.WithResourceTokenAuth(&credProvider),
		// gremcos.WithAuth(username, password), // <- static password obtained and set only once at startup
		gremcos.WithLogger(logger),
		gremcos.NumMaxActiveConnections(10),
		gremcos.ConnectionIdleTimeout(time.Second*30),
		gremcos.MetricsPrefix("myservice"),
	)

	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create the cosmos connector")
	}
	return cosmos
}
