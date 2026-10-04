package main

import (
	"fmt"

	nex "github.com/NextendoNetwork/nextendo-nex"
)

// Utility/10 GetClubCurrentStatus takes a raw club GID. Its response decoder
// (0xfe2a0 -> 0x15f70, build 2e90f081) reads a versioned structure containing
// exactly one u32. This is the current online member count, not the roster size.
func strikersClubCurrentStatus(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	if len(req.Body) != 4 {
		return nex.NewRMCError(conn.Settings, req.Protocol, req.CallID, nex.ResultCoreInvalidArgument)
	}
	id := nex.NewStreamIn(req.Body, conn.Settings).U32()
	clubStore.Lock()
	members := make([]uint64, 0)
	if id != 0 {
		for _, p := range clubStore.State.Players {
			if p.ClubID == id {
				members = append(members, p.PID)
			}
		}
	}
	clubStore.Unlock()
	var online uint32
	for _, pid := range members {
		if conn.Endpoint.FindConnectionByPID(pid) != nil {
			online++
		}
	}
	out := nex.NewStreamOut(conn.Settings)
	out.U8(0)
	out.U32(4)
	out.U32(online)
	fmt.Printf("[Strikers presence] club=%d online=%d members=%d\n", id, online, len(members))
	return nex.NewRMCSuccess(conn.Settings, req.Protocol, req.Method, req.CallID, out.Bytes())
}
