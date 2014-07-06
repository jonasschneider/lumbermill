package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/heroku/slog"
)

const (
	PointChannelCapacity = 100000
	HashRingReplication  = 20 // TODO: Needs to be determined
	PostersPerHost       = 6
)

const (
	Router = iota
	EventsRouter
	DynoMem
	DynoLoad
	EventsDyno
	numSeries
)

var (
	connectionCloser = make(chan struct{})

	chanGroups = make([]*ChanGroup, 0)

	seriesNames = []string{"router", "events.router", "dyno.mem", "dyno.load", "events.dyno"}

	seriesColumns = [][]string{
		[]string{"time", "id", "status", "service"}, // Router
		[]string{"time", "id", "code"},              // EventsRouter
		[]string{"time", "id", "source", "memory_cache", "memory_pgpgin", "memory_pgpgout", "memory_rss", "memory_swap", "memory_total", "dynoType"}, // DynoMem
		[]string{"time", "id", "source", "load_avg_1m", "load_avg_5m", "load_avg_15m", "dynoType"},                                                   // DynoLoad
		[]string{"time", "id", "what", "type", "code", "message", "dynoType"},                                                                        // DynoEvents
	}

	hashRing = NewHashRing(HashRingReplication, nil)

	Debug = os.Getenv("DEBUG") == "true"

	User     = os.Getenv("USER")
	Password = os.Getenv("PASSWORD")
	RiemannPrefix = os.Getenv("RIEMANN_PREFIX")
)

func LogWithContext(ctx slog.Context) {
	ctx.Add("app", "lumbermill")
	log.Println(ctx)
}


// Health Checks, so just say 200 - OK
// TODO: Actual healthcheck
func serveHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func main() {
	port := os.Getenv("PORT")

	group := NewChanGroup("riemann", PointChannelCapacity)
	chanGroups = append(chanGroups, group)
	poster := NewRiemannPoster(os.Getenv("RIEMANN_ADDRESS"), group)
	go poster.Run()

	hashRing.Add(chanGroups...)

	// Some statistics about the channels this way we can see how full they are getting
	go func() {
		for {
			ctx := slog.Context{}
			time.Sleep(10 * time.Second)
			for _, group := range chanGroups {
				group.Sample(ctx)
			}
			LogWithContext(ctx)
		}
	}()

	// Every 5 minutes, signal that the connection should be closed
	// This should allow for a slow balancing of connections.
	go func() {
		for {
			time.Sleep(5 * time.Minute)
			connectionCloser <- struct{}{}
		}
	}()

	http.HandleFunc("/drain", serveDrain)
	http.HandleFunc("/health", serveHealth)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
