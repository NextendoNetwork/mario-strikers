package main

import (
	"fmt"
	nex "github.com/NextendoNetwork/nextendo-nex"
)

// Striker Rankings uses standard Ranking.GetRanking, not Ranking2.
// Until match results are recorded, the leaderboard is genuinely empty.
// RankingResult still needs its structure envelope, total, and since_time;
// a bare empty list is not a valid response.
func strikersRankingHandler() nex.RMCHandler {
	fallback := nex.RankingHandler()
	return func(c *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
		if req.Method != 9 {
			return fallback(c, req)
		}
		in := nex.NewStreamIn(req.Body, c.Settings)
		mode, category := in.U8(), in.U32()
		in.U8() // RankingOrderParam version
		order := in.Substream()
		order.U8() // order calculation
		order.U8() // group index
		order.U8() // group count
		order.U8() // time scope
		offset, count := order.U32(), order.U8()
		in.U64() // unique ID
		in.PID()
		if in.Err() != nil || order.Err() != nil {
			return nex.NewRMCError(c.Settings, nex.ProtocolRanking, req.CallID, nex.ResultCoreInvalidArgument)
		}
		out := nex.NewStreamOut(c.Settings)
		out.U8(0) // RankingResult version
		out.U32(16)
		out.U32(0) // entries
		out.U32(0) // total
		out.U64(0) // no ranking period yet
		fmt.Printf("[Strikers Ranking] GetRanking pid=%d mode=%d category=%d offset=%d count=%d total=0\n", c.PID, mode, category, offset, count)
		return nex.NewRMCSuccess(c.Settings, nex.ProtocolRanking, req.Method, req.CallID, out.Bytes())
	}
}
