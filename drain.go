package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/bmizerany/lpx"
	"github.com/heroku/slog"
	"github.com/kr/logfmt"
)

var (
	TokenPrefix = []byte("t.")
	Heroku      = []byte("heroku")
)

func checkAuth(r *http.Request) error {
	header := r.Header.Get("Authorization")
	if header == "" {
		return errors.New("Authorization required")
	}
	headerParts := strings.SplitN(header, " ", 2)
	if len(headerParts) != 2 {
		return errors.New("Authorization header is malformed")
	}

	method := headerParts[0]
	if method != "Basic" {
		return errors.New("Only Basic Authorization is accepted")
	}

	encodedUserPass := headerParts[1]
	decodedUserPass, err := base64.StdEncoding.DecodeString(encodedUserPass)
	if err != nil {
		return errors.New("Authorization header is malformed")
	}

	userPassParts := bytes.SplitN(decodedUserPass, []byte{':'}, 2)
	if len(userPassParts) != 2 {
		return errors.New("Authorization header is malformed")
	}

	user := userPassParts[0]
	pass := userPassParts[1]

	if string(user) != User {
		return errors.New("Unknown user")
	}
	if string(pass) != Password {
		return errors.New("Incorrect token")
	}

	return nil
}

// Dyno's are generally reported as "<type>.<#>"
// Extract the <type> and return it
func dynoType(what string) string {
	s := strings.Split(what, ".")
	return s[0]
}

// "Parse tree" from hell
func serveDrain(w http.ResponseWriter, r *http.Request) {
	ctx := slog.Context{}
	defer func() { LogWithContext(ctx) }()
	w.Header().Set("Content-Length", "0")

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		ctx.Count("errors.drain.wrong.method", 1)
		return
	}

	id := r.Header.Get("Logplex-Drain-Token")

	/*if id == "" {
		if err := checkAuth(r); err != nil {
			w.WriteHeader(http.StatusForbidden)
			ctx.Count("errors.auth.failure", 1)
			return
		}
	}*/

	ctx.Count("batch", 1)

	parseStart := time.Now()
	lp := lpx.NewReader(bufio.NewReader(r.Body))

	for lp.Next() {
		ctx.Count("lines.total", 1)
		header := lp.Header()

		// If the syslog App Name Header field containts what looks like a log token,
		// let's assume it's an override of the id and we're getting the data from the magic
		// channel
		if bytes.HasPrefix(header.Name, TokenPrefix) {
			id = string(header.Name)
		}

		// If we still don't have an id, throw an error and try the next line
		if id == "" {
			ctx.Count("errors.token.missing", 1)
			continue
		}

		chanGroup := hashRing.Get(id)

		msg := lp.Bytes()
		switch {
		case bytes.Equal(header.Name, Heroku), bytes.HasPrefix(header.Name, TokenPrefix):
			t, e := time.Parse("2006-01-02T15:04:05.000000+00:00", string(lp.Header().Time))
			if e != nil {
				log.Printf("Error Parsing Time(%s): %q\n", string(lp.Header().Time), e)
				continue
			}
			timestamp := t.UnixNano() / int64(time.Microsecond)

			pid := string(header.Procid)
			switch pid {
			case "router":

				switch {
				// router logs with a H error code in them
				case bytes.Contains(msg, keyCodeH):
					ctx.Count("lines.router.error", 1)
					re := routerError{timestamp: timestamp, sourceDrain: id}
					err := logfmt.Unmarshal(msg, &re)
					if err != nil {
						log.Printf("logfmt unmarshal error: %s\n", err)
						continue
					}
					chanGroup.RouterErrors <- &re

				// likely a standard router log
				default:
					ctx.Count("lines.router", 1)
					rm := routerMsg{timestamp: timestamp, sourceDrain: id}
					err := logfmt.Unmarshal(msg, &rm)
					if err != nil {
						log.Printf("logfmt unmarshal error: %s\n", err)
						continue
					}
					chanGroup.RouterMsgs <- &rm
				}

				// Non router logs, so either dynos, runtime, etc
			default:
				switch {
				// Dyno error messages
				case bytes.HasPrefix(msg, dynoErrorSentinel):
					ctx.Count("lines.dyno.error", 1)
					de, err := parseBytesToDynoError(msg)
					if err != nil {
						log.Printf("Unable to parse dyno error message: %q\n", err)
					}
					de.timestamp = timestamp
					de.sourceDrain = id
					de.Dyno = string(lp.Header().Procid)

					chanGroup.DynoErrors <- &de

				// Dyno log-runtime-metrics memory messages
				case bytes.Contains(msg, dynoMemMsgSentinel):
					ctx.Count("lines.dyno.mem", 1)
					dm := dynoMemMsg{timestamp: timestamp, sourceDrain: id}
					err := logfmt.Unmarshal(msg, &dm)
					if err != nil {
						log.Printf("logfmt unmarshal error: %s\n", err)
						continue
					}

					chanGroup.DynoMemMsgs <- &dm

				// Dyno log-runtime-metrics load messages
				case bytes.Contains(msg, dynoLoadMsgSentinel):
					ctx.Count("lines.dyno.load", 1)
					dl := dynoLoadMsg{timestamp: timestamp, sourceDrain: id}
					err := logfmt.Unmarshal(msg, &dl)
					if err != nil {
						log.Printf("logfmt unmarshal error: %s\n", err)
						continue
					}

					chanGroup.DynoLoadMsgs <- &dl

				// unknown
				default:
					ctx.Count("lines.unknown.heroku", 1)
					if Debug {
						log.Printf("Unknown Heroku Line - Header: PRI: %s, Time: %s, Hostname: %s, Name: %s, ProcId: %s, MsgId: %s - Body: %s",
							header.PrivalVersion,
							header.Time,
							header.Hostname,
							header.Name,
							header.Procid,
							header.Msgid,
							string(msg),
						)
					}
				}
			}

		// non heroku lines
		default:
			ctx.Count("lines.unknown.user", 1)
			if Debug {
				log.Printf("Unknown User Line - Header: PRI: %s, Time: %s, Hostname: %s, Name: %s, ProcId: %s, MsgId: %s - Body: %s",
					header.PrivalVersion,
					header.Time,
					header.Hostname,
					header.Name,
					header.Procid,
					header.Msgid,
					string(msg),
				)
			}
		}
	}
	ctx.MeasureSince("lines.parse.time", parseStart)

	// If we are told to close the connection after the reply, do so.
	select {
	case <-connectionCloser:
		w.Header().Set("Connection", "close")
	default:
		//Nothing
	}

	w.WriteHeader(http.StatusNoContent)
}
