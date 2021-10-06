package query

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/gocolly/colly/v2"
)

func WikiEventByYear(year string) {
	// Instantiate default collector
	c := colly.NewCollector(
		colly.AllowedDomains("en.wikipedia.org", "wikidata.org"),
		colly.MaxDepth(1),
		colly.AllowURLRevisit(),
	)

	//one id a time
	vidCh := make(chan string, 1)

	// On visite vertex attempt success
	c.OnHTML("li#t-wikibase", func(li *colly.HTMLElement) {
		a := li.DOM.Children().Filter("a")
		href, _ := a.Attr("href")
		sp := strings.Split(href, "Q")
		pkQ := sp[len(sp)-1]
		vId := <-vidCh
		if !cosmosExistV(vId) {
			label := readInputV(fmt.Sprintf("Enter {label} of <%s> : ", vId))
			if label != "" {
				cosmosAddV(label, pkQ, vId)
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
				endEvents = true
			}
			if !endEvents {
				if s.Nodes[0].Data == "h3" {
					eventBy = strings.ToLower(strings.ReplaceAll(strings.Split(s.Text(), "[")[0], " ", "_"))
					Logger.Info().Msgf("%v\t%v", s.Nodes[0].Data, eventBy)
				}
				if s.Nodes[0].Data == "h4" {
					eventByWhat = strings.Split(strings.Split(s.Text(), "[")[0], ",")[0]
					Logger.Info().Msgf("%v\t%v", s.Nodes[0].Data, eventByWhat)
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
		Logger.Debug().Msgf("Visiting %v", r.URL.String())
	})

	visitVertexAttempt(vidCh, year, "https://en.wikipedia.org/wiki/"+year, c)
}

func visitVertexAttempt(vid chan string, vId string, link string, c *colly.Collector) {
	// https://gobyexample.com/non-blocking-channel-operations
	select {
	case vIdold := <-vid:
		Logger.Debug().Msgf("Blocked vid channel : %s ... New vertex wikipedia link?", vIdold)
		vIdStdIn := readInputV("Enter new <vertex> : ")
		if vIdStdIn != "" {
			println(vIdStdIn + "....visiting to attempt")
			c.Visit("https://en.wikipedia.org/wiki/" + vIdStdIn)
			println(vIdStdIn + "....visiting attemptted")
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
	Logger.Info().Msgf("%v", li.Text())
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
		go cosmosAddE(label, fromV, toV, eProperties)
		label, fromV, toV = readInputE("Edge {label};<fromV>;<toV>: ")
	}

	// add general edges for this event
	for by, byWhat := range eProperties {
		go cosmosAddE_x2o(by, byWhat, vertices)
	}
}

func readInputE(prompt string) (string, string, string) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt)
	text, err := reader.ReadString('\n')
	if err != nil {
		Logger.Panic().Msgf(err.Error())
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
		Logger.Panic().Msgf(err.Error())
	}
	text = strings.TrimSpace(text)
	return text
}
