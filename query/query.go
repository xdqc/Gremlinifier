package query

import (
	"fmt"

	"github.com/rs/zerolog"
	gremcos "github.com/supplyon/gremcos"
	"github.com/supplyon/gremcos/api"
)

var Cosmos gremcos.Cosmos
var Logger zerolog.Logger

func QueryCosmosEventBy(targetV string) {

	g := api.NewGraph("g")
	query := g.VByStr(targetV).InE().OutV().OutE()
	Logger.Info().Msgf("Query: %s", query)

	res, err := Cosmos.ExecuteQuery(query)

	if err != nil {
		Logger.Error().Err(err).Msg("Failed to execute a gremlin command")
		return
	}

	printResponses(api.ResponseArray(res), Logger)
}

func cosmosExistV(id string) bool {
	query := api.NewGraph("g").VByStr(id)
	res, err := Cosmos.ExecuteQuery(query)
	if err != nil {
		Logger.Error().Err(err).Msg("Failed to execute a gremlin command")
	}
	vertices, err := api.ResponseArray(res).ToVertices()
	if err != nil {
		Logger.Error().Err(err).Msg("Failed to resolve cosmos response")
	}
	return len(vertices) > 0
}

// Add V if not exist
// https://tinkerpop.apache.org/docs/3.3.2/reference/#inject-step
// https://tinkerpop.apache.org/docs/3.3.2/reference/#coalesce-step
func cosmosAddV(label string, pkq string, id string) {
	queryStr := fmt.Sprintf(
		"g.inject(0).coalesce(__.V('%s'), __.addV('%s').property('q','%s').property('id','%s'))",
		id, label, pkq, id)

	// Logger.Warn().Msgf("addV Query: %s", queryStr)

	_, err := Cosmos.Execute(queryStr)
	if err != nil {
		Logger.Error().Err(err).Msg("Failed to execute a gremlin command")
		return
	}

	// printResponses(api.ResponseArray(res), Logger)
}

func cosmosAddE(label string, fromV string, toV string) {
	queryStr := fmt.Sprintf(`g.V('%s').coalesce(
    __.outE('%s').where(inV().hasId('%s')),
    __.addE('%s').to(g.V('%s')))`,
		fromV, label, toV, label, toV)

	// Logger.Warn().Msgf("addE Query: %s", queryStr)

	_, err := Cosmos.Execute(queryStr)
	if err != nil {
		Logger.Error().Err(err).Msg("Failed to execute a gremlin command")
		return
	}

	// printResponses(api.ResponseArray(res), Logger)
}

// Add edges from many to one
// if any of Edges[many to one] exists, don't add
func cosmosAddE_x2o(label string, toV string, fromVs ...string) {
	queryStr := "g.V().has('id', within("
	for _, fromV := range fromVs {
		queryStr += fmt.Sprintf("'%s',", fromV)
	}
	queryStr += fmt.Sprintf(`)).as('items').V('%s') 
  .coalesce(__.inE('%s').where(outV().as('items')), 
            __.addE('%s').from('items'))`,
		toV, label, label)

	// Logger.Warn().Msgf("addE_x2o Query: %s", queryStr)

	_, err := Cosmos.Execute(queryStr)
	if err != nil {
		Logger.Error().Err(err).Msg("Failed to execute a gremlin command")
		return
	}

	// printResponses(api.ResponseArray(res), Logger)
}

func printResponses(responses api.ResponseArray, logger zerolog.Logger) {
	properties, err := responses.ToProperties()
	if err == nil {
		logger.Info().Msgf("Received Properties: %v", len(properties))
		for _, p := range properties {
			logger.Info().Msgf("%v", p)
		}
	}
	vertices, err := responses.ToVertices()
	if err == nil {
		logger.Info().Msgf("Received Vertices: %v", len(vertices))
		for _, v := range vertices {
			logger.Info().Msgf("%-8v\t%v", v.ID, v.Label)
			for _, p := range v.Properties {
				logger.Info().Msgf("\t\t%v", p[0])
			}
		}
	}
	edges, err := responses.ToEdges()
	if err == nil {
		logger.Info().Msgf("Received Edges: %v", len(edges))
		for _, e := range edges {
			logger.Info().Msgf("%16v:%-10v\t-%-10v->\t%10v:%-10v\t%v %v", e.OutV, e.OutVLabel, e.Label, e.InV, e.InVLabel, e.Type, e.ID)
		}
	}
	if properties == nil && vertices == nil && edges == nil {
		values, err := responses.ToValues()
		if err == nil {
			logger.Info().Msgf("Received Values: %v", values)
		}
	}
}
