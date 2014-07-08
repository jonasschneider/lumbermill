package main

import (
	"fmt"
	"log"
	"time"

	"github.com/amir/raidman"
	"github.com/heroku/slog"
)

type RiemannPoster struct {
	chanGroup      *ChanGroup
	name           string
	riemann        *raidman.Client
	riemannAddress string
}

func NewRiemannPoster(address string, chanGroup *ChanGroup) *RiemannPoster {
	riemann, err := raidman.Dial("tcp", address)

	if err != nil {
		panic(err)
	}

	return &RiemannPoster{
		chanGroup:      chanGroup,
		riemann:        riemann,
		riemannAddress: address,
	}
}

func (p *RiemannPoster) Run() {
	for {
		select {
		case rm, open := <-p.chanGroup.RouterMsgs:
			if !open {
				break
			}

			// Method    string
			// Path      string
			// Host      string
			// RequestId string
			// Fwd       string
			// Dyno      string
			// Connect   int
			// Service   int
			// Status    int
			// Bytes     int

			// timestamp int64
			// sourceDrain string

			event := &raidman.Event{
				Host:        RiemannPrefix + "router",
				Service:     rm.Host + " heroku latency",
				Metric:      rm.Connect + rm.Service,
				Ttl:         300,
				Time:        rm.timestamp/1e6,
				Description: fmt.Sprintf("%s %s in %dms by %s\n\nHost: %s\nRequest: %s", rm.Method, rm.Path, rm.Service, rm.Dyno, rm.Host, rm.RequestId),
				Attributes: map[string]string{
					"method": rm.Method,
					"path":   rm.Path,
					"status": string(rm.Status),

					"request_host": rm.Host,
					"request_id":   rm.RequestId,
					"fwd":          rm.Fwd,
					"dyno":         rm.Dyno,
					"bytes":        string(rm.Bytes),

					"logplex_source_id": rm.sourceDrain,
				},
			}

			p.deliver(event)

		case re, open := <-p.chanGroup.RouterErrors:
			if !open {
				break
			}

			// At        string
			// Code      string
			// Desc      string
			// Method    string
			// Host      string
			// Fwd       string
			// Dyno      string
			// Path      string
			// RequestId string
			// Connect   int
			// Service   int
			// Status    int
			// Bytes     int
			// Sock      string

			// timestamp int64
			// sourceDrain string

			event := &raidman.Event{
				State:       "error",
				Host:        RiemannPrefix + "router",
				Service:     "heroku_request_error",
				Ttl:         300,
				Time:        re.timestamp/1e6/1e6,
				Description: fmt.Sprintf("Error %s (%s) at %s for %s", re.Code, re.Desc, re.At, re.Host),
				Attributes: map[string]string{
					"at":     re.At,
					"method": re.Method,
					"path":   re.Path,
					"status": string(re.Status),

					"host":       re.Host,
					"request_id": re.RequestId,
					"fwd":        re.Fwd,
					"dyno":       re.Dyno,
					"bytes":      string(re.Bytes),

					"logplex_source_id": re.sourceDrain,
				},
			}

			p.deliver(event)

		case dm, open := <-p.chanGroup.DynoMemMsgs:
			if !open {
				break
			}

			// Source        string
			// Dyno          string
			// MemoryTotal   float64
			// MemoryRSS     float64
			// MemoryCache   float64
			// MemorySwap    float64
			// MemoryPgpgin  int
			// MemoryPgpgout int

			// timestamp int64
			// sourceDrain string

			event := &raidman.Event{
				Host:    RiemannPrefix + dm.Source,
				Service: "memory",
				Ttl:     300,
				Time:    dm.timestamp/1e6,
				Metric:  dm.MemoryTotal * 1e6,
				Description: fmt.Sprintf("%s used (%s RSS, %s swap, %s cached)",
					ByteSize(dm.MemoryTotal*1e6).String(),
					ByteSize(dm.MemoryRSS*1e6).String(),
					ByteSize(dm.MemorySwap*1e6).String(),
					ByteSize(dm.MemoryCache*1e6).String(),
				),
				Attributes: map[string]string{
					"logplex_source_id": dm.sourceDrain,
					"dyno":              dm.Dyno,
				},
			}

			p.deliver(event)

			event2 := &raidman.Event{
				Host:        RiemannPrefix + dm.Source,
				Service:     "memory_swap",
				Ttl:         300,
				Time:        dm.timestamp/1e6,
				Metric:      dm.MemorySwap * 1e6,
				Description: fmt.Sprintf("%s of swap used", ByteSize(dm.MemorySwap*1e6).String()),
				Attributes: map[string]string{
					"logplex_source_id": dm.sourceDrain,
					"dyno":              dm.Dyno,
				},
			}

			p.deliver(event2)

			event3 := &raidman.Event{
				Host:        RiemannPrefix + dm.Source,
				Service:     "memory_swap_pagecount",
				Ttl:         300,
				Time:        dm.timestamp/1e6,
				Metric:      dm.MemoryPgpgin + dm.MemoryPgpgout,
				Description: fmt.Sprintf("%d page ins, %d page outs", dm.MemoryPgpgin, dm.MemoryPgpgout),
				Attributes: map[string]string{
					"logplex_source_id": dm.sourceDrain,
					"dyno":              dm.Dyno,
				},
			}

			p.deliver(event3)

		case dl, open := <-p.chanGroup.DynoLoadMsgs:
			if !open {
				break
			}

			// Source       string
			// Dyno         string
			// LoadAvg1Min  float64
			// LoadAvg5Min  float64
			// LoadAvg15Min float64

			// timestamp int64
			// sourceDrain string

			event := &raidman.Event{
				Host:    RiemannPrefix + dl.Source,
				Service: "load",
				Ttl:     300,
				Time:    dl.timestamp/1e6,
				Metric:  dl.LoadAvg1Min,
				Description: fmt.Sprintf("load %f.2 %f.2 %f.2",
					dl.LoadAvg1Min,
					dl.LoadAvg5Min,
					dl.LoadAvg15Min,
				),
				Attributes: map[string]string{
					"logplex_source_id": dl.sourceDrain,
					"dyno":              dl.Dyno,
				},
			}

			p.deliver(event)

		}
	}
}

func (p *RiemannPoster) deliver(event *raidman.Event) {
	ctx := slog.Context{}
	defer func() { LogWithContext(ctx) }()

	start := time.Now()

	log.Printf("sending %#v\n", *event)

	err := p.riemann.Send(event)

	if err != nil {
		log.Println(ctx, "delivery error, trying to reconnect:", err)
		new_riemann, err := raidman.Dial("tcp", p.riemannAddress)
		if err == nil {
			log.Println("reconnected!")
			p.riemann = new_riemann
		} else {
			log.Println("reconnection failed:", err)
		}
	}

	ctx.MeasureSince("riemann_poster.time", start)
}
