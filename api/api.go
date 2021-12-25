package api

import (
	"net/http"
	"os"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

type GremlinQuery struct {
	Query     string `json:"query"`
	NodeLimit int    `json:"nodeLimit"`
}

func postGremlinQuery(c *gin.Context) {
	var gq GremlinQuery
	if err := c.BindJSON(&gq); err != nil {
		c.IndentedJSON(http.StatusInternalServerError, err)
		return
	}
	res := ExecuteGremlinQuery(gq)
	c.IndentedJSON(http.StatusOK, res)
}

func get_port() string {
	port := ":3001"
	if val, ok := os.LookupEnv("FUNCTIONS_CUSTOMHANDLER_PORT"); ok {
		port = ":" + val
	}
	return port
}

func main() {
	r := gin.Default()
	r.Use(cors.Default())
	r.POST("/api/GremlinQuery", postGremlinQuery)
	r.GET("/api/heartbeat", func(c *gin.Context) {
		c.IndentedJSON(http.StatusOK, "healthy")
	})
	r.Run(get_port())
}
