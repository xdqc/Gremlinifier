package ingest

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/gocolly/colly/v2"
	q "github.com/xdqc/gremlinifier/query"
)

//
func WikiEventServiceByYear(yearInt int) {
	var year string
	if yearInt < 0 {
		year = fmt.Sprintf("%d BC", -yearInt)
	} else if yearInt > 0 {
		year = fmt.Sprintf("%d AD", yearInt)
	} else {
		q.Logger.Warn().Msg("No year ZERO")
		WikiEventServiceByYear(yearInt + 1)
	}
	wikiEventScrapByYear(year)
	time.Sleep(time.Second * 2)
	q.QueryCosmos(fmt.Sprintf("g.V('%s').inE().outV()", year))
	q.QueryCosmos(fmt.Sprintf("g.V('%s').inE().outV().outE().order().by('label')", year))
}

func wikiEventScrapByYear(year string) {
	// Instantiate default collector
	c := colly.NewCollector(
		colly.AllowedDomains("en.wikipedia.org", "wikidata.org"),
		colly.MaxDepth(1),
		colly.AllowURLRevisit(),
	)

	//one id a time
	vidCh := make(chan string, 1)

	// On visite vertex attempt success, add wikidata item of the vertex
	c.OnHTML("li#t-wikibase", func(li *colly.HTMLElement) {
		a := li.DOM.Children().Filter("a")
		href, _ := a.Attr("href")
		sp := strings.Split(href, "Q")
		pkQ := sp[len(sp)-1]
		vId := <-vidCh
		if !q.QueryCosmosExistV(vId) {
			//TODO: use wikidata query to get most appropriate label (wdt:P31?/wdt:P279*)

			// CONSTRUCT {
			// 	?item1 wdt:P279 ?item2.
			// 	?item1 rdfs:label ?item1Label.
			// 	?item2 rdfs:label ?item2Label.
			// }
			// WHERE {
			// 	SELECT ?item1 ?item2 ?item1Label ?item2Label
			// 	 WHERE {
			// 		wd:Q41137 (wdt:P31?/wdt:P279*) ?item1, ?item2.
			// 		FILTER(EXISTS { ?item1 wdt:P279 ?item2. })
			// 		SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
			// 	}
			// }

			// SELECT ?class ?classLabel ?superclass ?superclassLabel
			// WHERE
			// {
			// 		wd:Q41137 wdt:P31?/wdt:P279* ?class.
			// 		?class wdt:P279 ?superclass.
			// 		SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
			// }

			// The challenge is to pick the best superclass or hypernym as label

			label := readInputV(fmt.Sprintf("Enter {label} of <%s> : ", vId))
			if label != "" {
				q.CosmosAddV(label, pkQ, vId)
			}
		}
	})

	// On every elemens after <h2>Events</h2>
	c.OnHTML("h2 span#Events", func(e *colly.HTMLElement) {
		endEvents := false
		eventBy := ""
		eventByWhat := ""
		e.DOM.Parent().NextAll().Each(func(i int, s *goquery.Selection) {
			if s.Nodes[0].Data == "h2" {
				// only process dom element within this <h2>Event</h2>
				endEvents = true
			}
			if !endEvents {
				if s.Nodes[0].Data == "h3" {
					//Examples:
					// <h3>By place</h3>
					// <h3>By topic</h3>
					eventBy = strings.ToLower(strings.ReplaceAll(strings.Split(s.Text(), "[")[0], " ", "_"))
					q.Logger.Info().Msgf("%v\t%v", s.Nodes[0].Data, eventBy)
				}
				if s.Nodes[0].Data == "h4" {
					//Examples:
					// <h4>Europe</h4>
					// <h4>Middle East</h4>
					// <h4>Agriculture</h4>
					// <h4>Exploration</h4>
					eventByWhat = strings.Split(strings.Split(s.Text(), "[")[0], ",")[0]
					q.Logger.Info().Msgf("%v\t%v", s.Nodes[0].Data, eventByWhat)
					visitVertexAttempt(vidCh, eventByWhat, "https://en.wikipedia.org/wiki/"+eventByWhat, c)
				}
				if s.Nodes[0].Data == "ul" {
					processUlEvents(eventBy, eventByWhat, vidCh, c, year, s, e)
				}
			}
		})
	})

	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		q.Logger.Debug().Msgf("Visiting %v", r.URL.String())
	})

	visitVertexAttempt(vidCh, year, "https://en.wikipedia.org/wiki/"+year, c)
}

//Ensure predicated object is a valid wikidata item.
//Prevent recursive deadlock https://gobyexample.com/non-blocking-channel-operations
func visitVertexAttempt(vid chan string, vId string, link string, c *colly.Collector) {
	select {
	case vIdold := <-vid:
		q.Logger.Debug().Msgf("Blocked vid channel : %s ... New vertex wikipedia link?", vIdold)
		vIdStdIn := readInputV("Enter new <vertex> : ")
		if vIdStdIn != "" {
			println(vIdStdIn + "... attempt to visit ...")
			c.Visit("https://en.wikipedia.org/wiki/" + vIdStdIn)
			println(vIdStdIn + " visit complete.")
		}
	default:
		// Logger.Debug().Msg("vid channel is clear")
	}
	vid <- vId
	c.Visit(link)
}

func processUlEvents(eventBy string, eventByWhat string, vidCh chan string, c *colly.Collector, year string, s *goquery.Selection, e *colly.HTMLElement) {
	if eventBy == "" {
		vIdstdIn := readInputV("Enter [event_by] : ")
		if vIdstdIn != "" {
			eventBy = "by_" + vIdstdIn
		}
	}
	if eventByWhat == "" {
		vIdstdIn := readInputV(fmt.Sprintf("Enter %s <What> : ", eventBy))
		if vIdstdIn != "" {
			visitVertexAttempt(vidCh, vIdstdIn, "https://en.wikipedia.org/wiki/"+vIdstdIn, c)
		}
		eventByWhat = vIdstdIn
	}
	if eventBy != "" && eventByWhat != "" {
		eProperties := make(map[string]string)
		eProperties["by_year"] = year
		eProperties[eventBy] = eventByWhat
		s.Children().Each(func(i int, li *goquery.Selection) {
			processLiEvent(li, vidCh, eProperties, c, e)
		})
	}
}

func processLiEvent(li *goquery.Selection, vid chan string, eProperties map[string]string, c *colly.Collector, e *colly.HTMLElement) {
	q.Logger.Info().Msgf("%v", li.Text())
	vertices := make([]string, 0)

	// get link in each event
	li.ContentsFiltered("a").Each(func(i int, a *goquery.Selection) {
		href, _ := a.Attr("href")
		sp := strings.Split(href, "/")
		vertexId := strings.Replace(sp[len(sp)-1], "_", " ", -1)
		class, _ := a.Attr("class")
		isYear, _ := regexp.MatchString("^\\d{2,}", vertexId)
		if class == "new" || isYear {
			// skip red link
			return
		}
		vertices = append(vertices, vertexId)
		visitVertexAttempt(vid, vertexId, e.Request.AbsoluteURL(href), c)
	})

	// Wikipedia is lack of hyperlink?
	vIdstdIn := readInputV(fmt.Sprintf("%s\n%v\nMore <vertex> ? ", li.Text(), vertices))
	if vIdstdIn != "" {
		vertices = append(vertices, vIdstdIn)
		visitVertexAttempt(vid, vIdstdIn, "https://en.wikipedia.org/wiki/"+vIdstdIn, c)
	}

	// add particular adge for this event
	label, fromV, toV := readInputE("Edge {label};<fromV>;<toV>: ")
	for label != "" && fromV != "" && toV != "" {
		go q.CosmosAddE(label, fromV, toV, eProperties)
		label, fromV, toV = readInputE("Edge {label};<fromV>;<toV>: ")
	}

	// add general edges for this event
	for by, byWhat := range eProperties {
		go q.CosmosAddE_x2o(by, byWhat, vertices)
	}
}

func readInputE(prompt string) (string, string, string) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt)
	text, err := reader.ReadString('\n')
	if err != nil {
		q.Logger.Panic().Msgf(err.Error())
	}
	texts := strings.Split(strings.TrimSpace(text), ";")
	if len(texts) != 3 {
		return "", "", ""
	}
	return texts[0], texts[1], texts[2]
}

func readInputV(prompt string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt)
	text, err := reader.ReadString('\n')
	if err != nil {
		q.Logger.Panic().Msgf(err.Error())
	}
	text = strings.TrimSpace(text)
	return text
}
