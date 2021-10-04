package query

import (
	"bufio"
	"fmt"
	"os"
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

	vid := make(chan string, 1) //one id a time

	c.OnHTML("li#t-wikibase", func(li *colly.HTMLElement) {
		a := li.DOM.Children().Filter("a")
		href, _ := a.Attr("href")
		sp := strings.Split(href, "Q")
		pkQ := sp[len(sp)-1]
		vId := <-vid
		if !cosmosExistV(vId) {
			label := readInputL(vId)
			if label != "" {
				cosmosAddV(label, pkQ, vId)
			}
		}
	})

	// On every a element which has href attribute call callback
	c.OnHTML("h2 span#Events", func(e *colly.HTMLElement) {
		Logger.Debug().Msg("here")
		endEvent := false
		eventBy := ""
		eventByWhat := ""
		e.DOM.Parent().NextAll().Each(func(i int, s *goquery.Selection) {
			if s.Nodes[0].Data == "h2" {
				endEvent = true
			}
			if !endEvent {
				if s.Nodes[0].Data == "h3" {
					eventBy = strings.ToLower(strings.ReplaceAll(strings.Split(s.Text(), "[")[0], " ", "_"))
					Logger.Info().Msgf("%v\t%v", s.Nodes[0].Data, eventBy)
				}
				if s.Nodes[0].Data == "h4" {
					eventByWhat = strings.Split(s.Text(), "[")[0]
					Logger.Info().Msgf("%v\t%v", s.Nodes[0].Data, eventByWhat)
					vid <- eventByWhat
					c.Visit(e.Request.AbsoluteURL("https://en.wikipedia.org/wiki/" + eventByWhat))
				}
				if s.Nodes[0].Data == "ul" {
					s.Children().Each(func(i int, li *goquery.Selection) {
						addCosmosEvent(li, vid, c, e, eventBy, eventByWhat, year)
					})
				}
			}
		})
	})

	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		Logger.Debug().Msgf("Visiting %v", r.URL.String())
	})

	vid <- year
	c.Visit("https://en.wikipedia.org/wiki/" + year)
}

func addCosmosEvent(li *goquery.Selection, vid chan string, c *colly.Collector, e *colly.HTMLElement, eventBy string, eventByWhat string, year string) {
	Logger.Info().Msgf("%v", li.Text())
	vertices := make([]string, 0)

	// get link in each event
	li.ContentsFiltered("a").Each(func(i int, a *goquery.Selection) {
		href, _ := a.Attr("href")
		class, _ := a.Attr("class")
		if class == "new" {
			// skip red link
			return
		}
		vertices = append(vertices, a.Text())
		vid <- a.Text()
		c.Visit(e.Request.AbsoluteURL(href))
	})

	// Wikipedia is lack of hyperlink?
	fmt.Printf("More vertex for event? %s\n", li.Text())
	vId := readInputV()
	if vId != "" {
		vertices = append(vertices, vId)
		vid <- vId
		c.Visit("https://en.wikipedia.org/wiki/" + vId)
	}

	// add particular adge
	label, fromV, toV := readInputE()
	for label != "" && fromV != "" && toV != "" {
		go cosmosAddE(label, fromV, toV)
		label, fromV, toV = readInputE()
	}
	// add general edges for this event
	go cosmosAddE_x2o("by_year", year, vertices...)
	if eventBy != "" {
		go cosmosAddE_x2o(eventBy, eventByWhat, vertices...)
	}
}

func readInputE() (string, string, string) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("Enter <label>;<fromV>;<toV>: ")
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

func readInputL(vId string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("Enter label for %s : ", vId)
	text, err := reader.ReadString('\n')
	if err != nil {
		Logger.Panic().Msgf(err.Error())
	}
	text = strings.TrimSpace(text)
	return text
}

func readInputV() string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("Enter vertex: ")
	text, err := reader.ReadString('\n')
	if err != nil {
		Logger.Panic().Msgf(err.Error())
	}
	text = strings.TrimSpace(text)
	return text
}
