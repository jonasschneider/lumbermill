package main

import (
	"time"
	"strconv"
	"log"

	"github.com/amir/raidman"
	"github.com/heroku/slog"
)

type RiemannPoster struct {
	chanGroup    *ChanGroup
	name         string
	riemann *raidman.Client
	riemannAddress string
}

func NewRiemannPoster(address string, chanGroup *ChanGroup) *RiemannPoster {
	riemann, err := raidman.Dial("tcp", address)

	if err != nil {
		panic(err)
	}

	return &RiemannPoster{
		chanGroup: chanGroup,
		riemann: riemann,
		riemannAddress: address,
	}
}

func (p *RiemannPoster) Run() {
	for {
		select {
		case point, open := <-p.chanGroup.points[Router]:
			if open {
				p.deliver(point, seriesColumns[0], "router")
			} else {
				break
			}
		case point, open := <-p.chanGroup.points[EventsRouter]:
			if open {
				p.deliver(point, seriesColumns[1], "router")
			} else {
				break
			}
		case point, open := <-p.chanGroup.points[DynoMem]:
			if open {
				p.deliver(point, seriesColumns[2], "dyno")
			} else {
				break
			}
		case point, open := <-p.chanGroup.points[DynoLoad]:
			if open {
				p.deliver(point, seriesColumns[3], "dyno")
			} else {
				break
			}
		case point, open := <-p.chanGroup.points[EventsDyno]:
			if open {
				p.deliver(point, seriesColumns[4], "dynomanager")
			} else {
				break
			}
		}
	}
}

func (p *RiemannPoster) deliver(point []interface{}, columns []string, prefix string) {
	ctx := slog.Context{}
	defer func() { LogWithContext(ctx) }()

	start := time.Now()

	event_host := "heroku"
	event_time := time.Now()
	attrs := make(map[string]string)

	for i := range point {
		entry := point[i]
		field := columns[i]

		if field == "source" {
			entry_str, ok := entry.(string)
			if ok {
				event_host = entry_str
			}
		}

		if field == "time" {
			nanos, ok := entry.(int64)
			if !ok {
				continue
			}
			event_time = time.Unix(nanos/1000, nanos)
		}

		if field == "id" {
			id, ok := entry.(string)
			if !ok {
				continue
			}
			attrs["id"] = id
		}
	}

	for i := range point {
		entry := point[i]
		field := columns[i]

		var metric interface{} // Riemann allows int, float32, float64
		var err error

		entry_str, ok := entry.(string)

		if field == "time" || field == "source" || field == "id" {
			continue
		}

		if ok {
			metric, err = strconv.Atoi(entry_str)

			if err != nil {
				log.Println(ctx, "atoi error while parsing metric from field",field,"with error",err)
				continue
			}
		} else {
			entry_float, ok := entry.(float64)
			if ok {
				metric = entry_float
			} else {
				entry_int, ok := entry.(int)
				if ok {
					metric = entry_int
				} else {
					log.Println(ctx, "field",field,"val",entry,"is neither int, float nor string -- confused")
					continue
				}
			}
		}

	  var event = &raidman.Event{
		  State: "success",
		  Host: RiemannPrefix+event_host,
		  Service: prefix+"_"+field,
		  Metric:	metric,
		  Ttl: 300,
		  Time: event_time.Unix(),
		  Attributes: attrs,
		}

		log.Printf("sending %#v\n", *event)

		err = p.riemann.Send(event)

		if err != nil {
			log.Println(ctx, "delivery error, trying to reconnect:",err)
			new_riemann, err := raidman.Dial("tcp", p.riemannAddress)
			if err == nil {
				log.Println("reconnected!")
				p.riemann = new_riemann
			} else {
				log.Println("reconnection failed:",err)
			}
		}
	}

	ctx.MeasureSince("riemann_poster.time", start)
}
