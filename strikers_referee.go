package main

import (
	"fmt"
	"sync"
	"time"

	nex "github.com/NextendoNetwork/nextendo-nex"
)

type strikersRound struct {
	id            uint64
	category, gid uint32
	pids          []uint64
	param         []byte
	created       time.Time
}

// StartRound's scalar reply alone does not release gameplay. In retail build
// 2e90f081, 0x657fbc handles event category 116 and 0x658058 sets the round ID
// and gameplay-start flags at +0x20/+0x22. The old successful zero-ID reply
// never delivered that event, leaving connected clients on the black screen.
func setupStrikersReferee(ep *nex.Endpoint, mm *nex.Matchmaking) {
	var mu sync.Mutex
	rounds := make(map[uint64]strikersRound)
	requests := make(map[[2]uint32]uint64)
	nextID := uint64(time.Now().UnixNano())
	fallback := strikersInitHandler(0x78)
	ep.Register(0x78, func(c *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
		fail := func() *nex.RMCMessage {
			return nex.NewRMCError(c.Settings, 0x78, req.CallID, nex.ResultCoreInvalidArgument)
		}
		out := nex.NewStreamOut(c.Settings)
		switch req.Method {
		case 1:
			in := nex.NewStreamIn(req.Body, c.Settings)
			in.U8()
			body := in.Substream()
			category, gid, count := body.U32(), body.U32(), body.U32()
			if in.Err() != nil || body.Err() != nil || count == 0 || count > 64 {
				return fail()
			}
			pids := make([]uint64, 0, count)
			seen := make(map[uint64]bool)
			for i := uint32(0); i < count; i++ {
				pid := body.PID()
				if pid == 0 || seen[pid] {
					return fail()
				}
				seen[pid] = true
				pids = append(pids, pid)
			}
			if body.Err() != nil || !seen[c.PID] {
				return fail()
			}
			valid := false
			for _, g := range mm.Snapshot() {
				if g.ID != gid {
					continue
				}
				members := make(map[uint64]bool)
				for _, pid := range g.Participants {
					members[pid] = true
				}
				valid = true
				for _, pid := range pids {
					if !members[pid] {
						valid = false
					}
				}
				break
			}
			if !valid {
				fmt.Printf("[Strikers referee] rejected start pid=%d gid=%d participants=%v\n", c.PID, gid, pids)
				return fail()
			}
			key := [2]uint32{c.ID, req.CallID}
			mu.Lock()
			// Round bookkeeping is transient, like the matchmaking sessions.
			for id, r := range rounds {
				if time.Since(r.created) > 24*time.Hour {
					delete(rounds, id)
				}
			}
			for k, id := range requests {
				if _, ok := rounds[id]; !ok {
					delete(requests, k)
				}
			}
			id, repeat := requests[key]
			if !repeat {
				nextID++
				id = nextID
				requests[key] = id
				rounds[id] = strikersRound{id: id, category: category, gid: gid, pids: pids, param: append([]byte(nil), req.Body...), created: time.Now()}
			}
			mu.Unlock()
			if !repeat {
				for _, pid := range pids {
					if target := ep.FindConnectionByPID(pid); target != nil {
						nex.SendNotification(target, &nex.NotificationEvent{PIDSource: c.PID, Type: 116000, Param1: id})
						fmt.Printf("[Strikers referee] round-start round=%d gid=%d -> pid=%d\n", id, gid, pid)
					}
				}
			}
			out.U64(id)
		case 2, 5:
			if len(req.Body) != 8 {
				return fail()
			}
			id := nex.NewStreamIn(req.Body, c.Settings).U64()
			mu.Lock()
			r, ok := rounds[id]
			mu.Unlock()
			allowed := false
			for _, pid := range r.pids {
				if pid == c.PID {
					allowed = true
				}
			}
			if !ok || !allowed {
				return fail()
			}
			if req.Method == 2 {
				out.Write(r.param)
			} else {
				out.U32(uint32(len(r.pids)))
				for _, pid := range r.pids {
					out.U64(pid)
				}
			}
		default:
			return fallback(c, req)
		}
		return nex.NewRMCSuccess(c.Settings, 0x78, req.Method, req.CallID, out.Bytes())
	})
}
