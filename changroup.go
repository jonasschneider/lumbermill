package main

import (
	"github.com/heroku/slog"
)

type ChanGroup struct {
	Name string

	DynoErrors chan *dynoError
	DynoMemMsgs chan *dynoMemMsg
	DynoLoadMsgs chan *dynoLoadMsg

	RouterMsgs chan *routerMsg
	RouterErrors chan *routerError
}

func NewChanGroup(name string, chanCap int) *ChanGroup {
	group := &ChanGroup{Name: name}
	group.DynoErrors = make(chan *dynoError, chanCap)
	group.DynoMemMsgs = make(chan *dynoMemMsg, chanCap)
	group.DynoLoadMsgs = make(chan *dynoLoadMsg, chanCap)
	group.RouterMsgs = make(chan *routerMsg, chanCap)
	group.RouterErrors = make(chan *routerError, chanCap)

	return group
}

func (group *ChanGroup) Sample(ctx slog.Context) {
	ctx.Add("source", group.Name)
	ctx.Sample("points.DynoErrors.pending", len(group.DynoErrors))
	ctx.Sample("points.DynoMemMsgs.pending", len(group.DynoMemMsgs))
	ctx.Sample("points.DynoLoadMsgs.pending", len(group.DynoLoadMsgs))
	ctx.Sample("points.RouterMsgs.pending", len(group.RouterMsgs))
	ctx.Sample("points.RouterErrors.pending", len(group.RouterErrors))
}
