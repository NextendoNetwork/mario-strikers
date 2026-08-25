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
// TODO: fill the 62/65 struct with real fields (u64, u16, u32, list<u32>, qBuffer, DateTimes,
// version-gated u32 run — decoder 0x1b930) so real matchmaking data can flow; zeros only parse.
// Response kinds recovered from the game binary's NEX decoders (no measured). SINGLE-STRUCT
// methods need a versioned-struct envelope; everything else (LIST / LIST<u32> / scalar) is
// fine with an empty list (u32=0). Club family: 55/56/58/59 = create/update/join/get ONE club
// (single struct, decoder 0x15970); 57/60/71/73 = find/list clubs (LIST<club>, decoder 0x1f534)
// — a struct envelope there is read as a huge element count and blows up, so they must be lists.
// 62/65 = online-init struct (0x1b930); 76 (0x4c) & 80 (0x50) = other single structs.
var strikersStructMethods = map[uint32]bool{
	62: true, 65: true, // online-init struct (decoder 0x1b930)
	76: true, 80: true, // other single-struct methods
} // 55 create + 56/58/59 update/join/get one club are handled specially (real club struct, not zero).
// 73 is a LIST<club>, NOT here — it must return an empty list, not a struct envelope.
//
// 63 (0x3f) is a LIST, NOT a struct: answering it with a struct/zero-struct makes the client read
// the [ver][len] header as a huge element count -> Core::BufferOverflow (0x8001000F). An empty list
// (u32=0) decodes fine and the game reaches a LATER stage (NEXManagerState 20) where the real
// roster blocker lives (Core::Unknown, module 121) — diagnosed via full service logging, not by
// guessing 63's format. So 63 stays on the empty-list default below.

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

// setupStrikersMatchmakeExt wraps the base MatchmakeExtension (0x6D) handler.
// Strikers uses game-specific methods above the common set (the base tops out at
// method 53; Strikers calls e.g. 0x6d.62) that the base answers with notImplemented
// — which the game treats as a fatal "network error". We instead LOG the request
// bytes (to reverse each unknown method from its wire structure) and answer
// empty-success so the game PROCEEDS and reveals its next call. Every common method
// the base already implements still goes through it unchanged. Capture-then-implement
// loop, scoped to Strikers — no guessing baked in, just a probe that yields the bytes.
func setupStrikersMatchmakeExt(endpoint *nex.Endpoint, base nex.RMCHandler) {
	endpoint.Register(nex.ProtocolMatchmakeExtension, func(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
		// Methods 84-89 return "bool + list<u32>" (binary DDL, decoder 0x195f0): a lone u32=0
		// is a byte short (needs [bool][u32 count] ≥ 5B) and overflows. Answer [false][empty].
		if strikersBoolListMethods[req.Method] {
			out := nex.NewStreamOut(conn.Settings)
			out.Bool(false)
			out.U32(0)
			fmt.Printf("[Strikers 0x6d] method %d call=%d -> bool+empty-list\n", req.Method, req.CallID)
			return nex.NewRMCSuccess(conn.Settings, nex.ProtocolMatchmakeExtension, req.Method, req.CallID, out.Bytes())
		}
		// Method 55 (create club): return a REAL club (valid gid + owner) so the game accepts
		// the creation instead of retrying in a loop on an all-zero club.
		if req.Method == 55 {
			return strikersCreateClub(conn, req)
		}
		// Methods 56/58/59 (update/join/get one club): return the caller's REAL club struct.
		// A generic zero-struct here overflows the club decoder (0x15970) -> comm error on join.
		if req.Method == 56 || req.Method == 58 || req.Method == 59 {
			return strikersGetClub(conn, req)
		}
		// Method 73 (list clubs): return the caller's created clubs so the game sees the club
		// it just made (an empty list here makes it conclude the club doesn't exist -> error).
		if req.Method == 73 {
			return strikersListClubs(conn, req)
		}
		// Method 83 (club member roster): return the owner as a member so the roster shows 1/20
		// + a member card instead of 0/0. Pressing into that card makes the game fetch the
		// member's profile (Mii/nickname) via nn::friends GetProfileList. That IPC was stubbed
		// EMPTY in the stock emulator (ProfileImpl was a 0-byte struct) -> the game waited forever
		// for a valid profile -> soft-lock, which is why 83 was disabled. The custom emulator
		// build (mario strikers test/) now fills GetProfileList (accountId@0x00 + IsValid + the
		// account's pseudo), so the member card renders. 83 and the emulator fix ship together.
		if req.Method == 83 {
			return strikersClubRoster(conn, req)
		}
		// Struct-returning methods (62, 65, ...): answer with a versioned-structure envelope
		// [u8 version][u32 size][size zero bytes]. The client reads its fields as empty/default
		// and stays IN BOUNDS; an empty list (4 bytes) is too short -> it reads past -> overflow.
		// These are intercepted BEFORE the base handler (65 is handled by base as an empty list,
		// which is exactly what overflows for Strikers).
		if strikersStructMethods[req.Method] {
			fmt.Printf("[Strikers 0x6d] method %d (0x%x) call=%d bodyLen=%d -> zero-struct [struct method]\n",
				req.Method, req.Method, req.CallID, len(req.Body))
			return strikersZeroStruct(conn.Settings, nex.ProtocolMatchmakeExtension, req.Method, req.CallID)
		}
		resp := base(conn, req)
		if resp != nil && resp.IsError {
			// Other unknown methods: empty NEX list (Samy's default unblock for list-returning
			// game-specific methods with no active data — acnh custom_methods.go, no measured).
			out := nex.NewStreamOut(conn.Settings)
			out.U32(0)
			fmt.Printf("[Strikers 0x6d] UNHANDLED method=%d (0x%x) call=%d bodyLen=%d hex=%x -> empty-list (count=0)\n",
				req.Method, req.Method, req.CallID, len(req.Body), req.Body)
			return nex.NewRMCSuccess(conn.Settings, nex.ProtocolMatchmakeExtension, req.Method, req.CallID, out.Bytes())
		}
		return resp
	})
}

func strikersInitHandler(proto uint16) nex.RMCHandler {
	return func(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
		s := conn.Settings

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
