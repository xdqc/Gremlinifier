package api

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/supplyon/gremcos/interfaces"
	q "github.com/xdqc/gremlinifier/query"
)

//ViewModel vertex
type VisNode struct {
	Id         string                 `json:"id"`
	Label      string                 `json:"label"`
	Properties map[string]interface{} `json:"properties"`
	Edges      []VisEdge              `json:"edges"`
}

//ViewModel edge
type VisEdge struct {
	Id         string                 `json:"id"`
	Label      string                 `json:"label"`
	From       string                 `json:"from"`
	To         string                 `json:"to"`
	Properties map[string]interface{} `json:"properties"`
}

func ExecuteGremlinQuery(gq GremlinQuery) []VisNode {
	var query string
	if strings.HasSuffix(gq.Query, ".out()") {
		query = makeOutQuery(gq)
	} else if strings.HasSuffix(gq.Query, ".in()") {
		query = makeInQuery(gq)
	} else {
		query = makeSelfQuery(gq)
	}
	q.Logger.Debug().Msg(query)
	res := q.QueryCosmosRes(query)
	return ToVisNodes(res)
}

func ToVisNodes(resps []interfaces.Response) []VisNode {
	vs := make([]VisNode, 0)
	for _, res := range resps {
		if res.IsEmpty() {
			continue
		}
		var v []VisNode
		err := json.Unmarshal(res.Result.Data, &v)
		if err != nil {
			q.Logger.Err(err).Msgf("Query projection not matching VisNode: %v", string(res.Result.Data))
		}
		for _, vn := range v {
			for k, np := range vn.Properties {
				vn.Properties[k] = np.([]interface{})[0]
			}
		}
		vs = append(vs, v...)
	}
	return vs
}

func makeSelfQuery(gq GremlinQuery) string {
	return gq.Query + `.dedup()
		.project('id', 'label', 'properties')
		.by(__.id())
		.by(__.label())
		.by(__.valueMap())`
}

func makeInQuery(gq GremlinQuery) string {
	return fmt.Sprintf(`%s
		.limit(%d)
		.dedup()
		.as('node')
		.project('id', 'label', 'properties', 'edges')
		.by(__.id())
		.by(__.label())
		.by(__.valueMap())
		.by(__.outE().as('outEdge')
				.project('id', 'from', 'to', 'label', 'properties')
				.by(__.id())
				.by(select('node').id())
				.by(__.inV().id())
				.by(__.label())
				.by(__.valueMap())
				.fold()
		)
	`, gq.Query, gq.NodeLimit)
}

func makeOutQuery(gq GremlinQuery) string {
	return fmt.Sprintf(`%s
		.limit(%d)
		.dedup()
		.as('node')
		.project('id', 'label', 'properties', 'edges')
		.by(__.id())
		.by(__.label())
		.by(__.valueMap())
		.by(__.inE().as('inEdge')
				.project('id', 'from', 'to', 'label', 'properties')
				.by(__.id())
				.by(__.outV().id())
				.by(select('node').id())
				.by(__.label())
				.by(__.valueMap())
				.fold()
		)
	`, gq.Query, gq.NodeLimit)
}
