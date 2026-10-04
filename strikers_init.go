package main

// Strikers online-init handler for DataStore (0x73) + Utility (0x6E).
//
// We do NOT yet have a measured reference of Mario Strikers' online init, so —
// exactly as SSBU/S2/ACNH were brought up — every call to these two protocols is
// LOGGED (to diff against a real measured and implement iteratively) and answered
// with a valid empty response (count=0) so the game PROCEEDS and reveals what it
// calls next, instead of soft-locking on NotImplemented. 0x73.8 is answered with
// DataStore::NotFound (0x80690004), which NEX clients handle gracefully. This is
// the measured-then-implement method, on the closed-source NEXtendo core — no
// SSBU-specific measured bytes are replayed here.

import (
	"encoding/binary"
	"fmt"

	nex "github.com/NextendoNetwork/nextendo-nex"
)

// setupStrikersInit registers the logging + empty-list handler for DataStore
// (0x73) and Utility (0x6E) on the secure endpoint.
func setupStrikersInit(endpoint *nex.Endpoint) {
	loadClubs() // restore persisted clubs so a redeploy doesn't wipe them
	endpoint.Register(0x73, strikersInitHandler(0x73))
	endpoint.Register(0x6E, strikersInitHandler(0x6E))
	// Ranking2 (0x7A) + MatchmakeReferee (0x78): the game calls these during club setup.
	// Leaving them unregistered returns Core::NotImplemented (0x80010002) -> comm error (and
	// previously a crash). Register them so their methods get a real answer.
	endpoint.Register(0x7A, strikersInitHandler(0x7A))
	endpoint.Register(0x78, strikersInitHandler(0x78))
	fmt.Printf("[Strikers Init] handlers registered: DataStore(0x73) + Utility(0x6E) + Ranking2(0x7A) + MatchmakeReferee(0x78)\n")
}

// Response KINDS per method, recovered by reversing the game binary's NEX DDL /
// MatchmakeExtensionProtocolClient vtable (no measured). Methods that deserialize as a
// single STRUCTURE must be answered with a versioned-structure envelope; answering them
// with an empty list (4 bytes) is one byte short of the 5-byte struct header and raises
// Core::BufferOverflow (0x8001000F). The list/scalar methods are fine with an empty list:
//
//	0x6D.62 STRUCT | 0x6D.65 STRUCT (identical type to 62) | 0x6D.70 list | 0x6D.90 list<u32>
//	0x6D.91 scalar u32 | 0x6E.7 list | 0x6E.11 STRUCT
//
// Club and player methods have persistent handlers in strikers_club.go.
// These remaining season responses still use the existing placeholder implementation.
var strikersStructMethods = map[uint32]bool{76: true, 80: true}

// strikersBoolListMethods return "bool + list<u32>" (decoder 0x195f0), not a plain list —
// a lone u32=0 is one byte short of [bool][u32 count] and overflows.
var strikersBoolListMethods = map[uint32]bool{84: true, 85: true, 86: true, 88: true, 89: true}

// strikersInitStructMethods: per-protocol methods that return a single STRUCT (from the
// reversed decoder tables of Utility 0x6E / Ranking2 0x7A / MatchmakeReferee 0x78). Anything
// not listed here (and not void/scalar-u64) is treated as a list -> empty-list.
var strikersInitStructMethods = map[uint16]map[uint32]bool{
	0x6E: {10: true, 11: true},
	0x7A: {9: true, 11: true, 12: true, 14: true},
	0x78: {2: true, 3: true, 4: true, 7: true, 12: true, 14: true},
}

// strikersZeroStruct answers a struct-returning method with a NEX versioned-structure
// envelope: [u8 version=2][u32 length=256][256 zero bytes]. The client reads its struct's
// fields as empty/default and seeks to start+length afterwards, so it stays in bounds
// regardless of the exact field list (any empty list/qBuffer/string reads as count/len 0).
func strikersZeroStruct(s *nex.Settings, proto uint16, method, callID uint32) *nex.RMCMessage {
	const zeroLen = 256
	body := make([]byte, 5+zeroLen)
	body[0] = 2 // structure version
	binary.LittleEndian.PutUint32(body[1:5], zeroLen)
	return nex.NewRMCSuccess(s, proto, method, callID, body)
}

// Keep the Strikers-specific club protocol separate from common NEX matchmaking;
// anything the base answers with notImplemented is logged and given empty-success.
func setupStrikersMatchmakeExt(endpoint *nex.Endpoint, base nex.RMCHandler) {
	endpoint.Register(nex.ProtocolMatchmakeExtension, func(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
		switch req.Method {
		case 55:
			return strikersCreateClub(conn, req)
		case 56, 58, 59:
			return strikersGetClub(conn, req)
		case 62, 65:
			return strikersPlayerRequest(conn, req)
		case 63:
			return strikersClubPlayers(conn, req)
		case 73:
			return strikersListClubs(conn, req)
		case 72:
			return strikersClubCurrentStatus(conn, req)
		case 83:
			return strikersClubRoster(conn, req)
		}
		if strikersBoolListMethods[req.Method] {
			out := nex.NewStreamOut(conn.Settings)
			out.Bool(false)
			out.U32(0)
			return nex.NewRMCSuccess(conn.Settings, nex.ProtocolMatchmakeExtension, req.Method, req.CallID, out.Bytes())
		}
		if strikersStructMethods[req.Method] {
			return strikersZeroStruct(conn.Settings, nex.ProtocolMatchmakeExtension, req.Method, req.CallID)
		}
		resp := base(conn, req)
		if resp != nil && resp.IsError {
			out := nex.NewStreamOut(conn.Settings)
			out.U32(0)
			fmt.Printf("[Strikers 0x6d] UNHANDLED method=%d call=%d bodyLen=%d hex=%x -> empty-list\n", req.Method, req.CallID, len(req.Body), req.Body)
			return nex.NewRMCSuccess(conn.Settings, nex.ProtocolMatchmakeExtension, req.Method, req.CallID, out.Bytes())
		}
		return resp
	})
}

func strikersInitHandler(proto uint16) nex.RMCHandler {
	return func(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
		s := conn.Settings
		if proto == 0x6E && req.Method == 10 {
			return strikersClubCurrentStatus(conn, req)
		}

		// 0x73.8 = DataStore::GetMeta-style lookup; Nintendo answers NotFound and
		// the client continues. Mirror it so Strikers doesn't treat it as fatal.
		if proto == 0x73 && req.Method == 8 {
			fmt.Printf("[Strikers Init] 0x73.8 -> NotFound 0x80690004\n")
			return nex.NewRMCError(s, proto, req.CallID, 0x80690004)
		}

		// VOID — RMC success with an EMPTY body: Ranking2 0x7A.13 (a plain ack; returning an
		// error/struct there trips the game's crash path) and Utility 0x6E.14/15.
		if (proto == 0x7A && req.Method == 13) || (proto == 0x6E && (req.Method == 14 || req.Method == 15)) {
			fmt.Printf("[Strikers Init] 0x%02x.%d -> void (empty ack)\n", proto, req.Method)
			return nex.NewRMCSuccess(s, proto, req.Method, req.CallID, nil)
		}

		// SCALAR u64: MatchmakeReferee 0x78.1 (reads one u64).
		if proto == 0x78 && req.Method == 1 {
			out := nex.NewStreamOut(s)
			out.U64(0)
			return nex.NewRMCSuccess(s, proto, req.Method, req.CallID, out.Bytes())
		}

		// STRUCT — versioned-struct envelope. Per-protocol sets from the reversed decoder table:
		// Utility 10/11; Ranking2 9/11/12/14; MatchmakeReferee 2/3/4/7/12/14. (Utility 7/9 and
		// MMR 9/16/17 are LISTS -> the empty-list default below; Utility 9 was wrongly a struct.)
		if strikersInitStructMethods[proto][req.Method] {
			fmt.Printf("[Strikers Init] 0x%02x.%d -> zero-struct [struct]\n", proto, req.Method)
			return strikersZeroStruct(s, proto, req.Method, req.CallID)
		}

		// Everything else — LIST<scalar>/LIST<struct> (empty list is valid) and scalar u32.
		out := nex.NewStreamOut(s)
		out.U32(0)
		fmt.Printf("[Strikers Init] 0x%02x.%d -> empty-list\n", proto, req.Method)
		return nex.NewRMCSuccess(s, proto, req.Method, req.CallID, out.Bytes())
	}
}
