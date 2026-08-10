package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	nex "github.com/NextendoNetwork/nextendo-nex"
)

// strikersClub is Mario Strikers' "Club" structure (the type is literally named "Club" in
// the binary, extending Gathering). Byte-exact per the game's decoder 0x15970:
//
//	[u8 gVer][u32 gLen][ gathering fields ]  [u8 cVer][u32 cLen][ club fields ]
//
// It is a standard NEX derived-class structure — the Gathering base is serialized FIRST with
// its own header, then the Club level adds its own header + fields (two SEQUENTIAL levels, not
// a nested gathering). The core's Structure machinery emits exactly this framing.
//
// The club fields are version-gated. At cVer=0 the decoder reads ONLY these eight, then stops:
//
//	list<u32>, u32, Buffer(u32-len), String, String, u32, u32, DateTime(u64)
//
// (cVer>=1 adds a u32; >=2 adds list<u32>+u16+u16; >=3 a u32; >=4 a bool. We emit cVer=0 so the
// game never reads those, which is why an over-long / mis-versioned encode overflowed before.)
type strikersClub struct {
	nex.Gathering
	tags    []uint32
	num1    uint32
	emblem  []byte // field #3: the club emblem config (byte[0]=type, byte[2]=sub-index)
	str1    string
	str2    string
	num2    uint32
	num3    uint32
	created nex.DateTime
}

// Levels: inherited Gathering level (gVer=0) then the club level (cVer=0, fields 1-8 only).
func (c *strikersClub) Levels() []nex.Level {
	return append(c.Gathering.Levels(), nex.Level{
		Version: 0, // cVer=0 — the game reads only the always-present fields 1-8
		Save: func(o *nex.StreamOut) {
			nex.WriteList(o, c.tags, func(o *nex.StreamOut, v uint32) { o.U32(v) })
			o.U32(c.num1)
			o.Buffer(c.emblem)
			o.String(c.str1)
			o.String(c.str2)
			o.U32(c.num2)
			o.U32(c.num3)
			o.DateTime(c.created.Value())
		},
	})
}

var strikersClubGID uint32 = 0x10000000

// newStrikersClub builds a freshly-created club owned by the caller. A non-zero gathering id +
// owner is the minimum the game needs to accept a created club.
func newStrikersClub(ownerPID uint64, name string) *strikersClub {
	c := &strikersClub{}
	c.ID = atomic.AddUint32(&strikersClubGID, 1)
	c.OwnerPID = ownerPID
	c.HostPID = ownerPID
	c.MinParticipants = 1
	c.MaxParticipants = 20
	c.ParticipationPolicy = 98 // "anybody" — a valid non-zero policy
	c.PolicyArgument = 0
	c.Flags = 0
	c.State = 1
	c.Description = name
	c.created = nex.NowDateTime()
	// Club-level fields the club-list render path (PopulateClubJoinPanel @0x4e1200) consumes:
	//  - field #3 Buffer is the club EMBLEM config, NOT a generic buffer: byte[0]=emblem type
	//    (0 is valid), byte[2]=sub-index. Empty -> the emblem asset resolves to null and the
	//    panel memcpy's 24 bytes from it -> crash. A ≥10-byte zero config = valid default emblem.
	//  - field #1 tags is read as an indexed array (the game dereferences tags[1]); it must have
	//    ≥2 elements or the index read is out of bounds.
	c.emblem = make([]byte, 16) // emblem type 0 / sub-index 0 — a valid default
	c.tags = []uint32{0, 0}
	c.str1 = name
	c.str2 = name
	return c
}

// clubStore keeps created clubs in memory so the game can see, after creating one (55), its own
// club in the list (73). Without this the game creates a club, lists clubs, gets an empty list,
// decides the club doesn't exist, and errors.
var clubStore = struct {
	sync.Mutex
	byOwner map[uint64][]*strikersClub
}{byOwner: map[uint64][]*strikersClub{}}

// strikersCreateClub answers MatchmakeExtension 0x37 (55, create club) with a real club so the
// creation is accepted, and remembers it for the club list (73).
func strikersCreateClub(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	owner := uint64(conn.PID)
	club := newStrikersClub(owner, "Nextendo")

	clubStore.Lock()
	clubStore.byOwner[owner] = append(clubStore.byOwner[owner], club)
	clubStore.Unlock()
	saveClubs() // persist so a server restart doesn't lose it

	out := nex.NewStreamOut(conn.Settings)
	nex.WriteStructure(out, club)
	fmt.Printf("[Strikers 0x6d] method 55 create-club -> gid=%d owner=%d (%dB)\n", club.ID, owner, len(out.Bytes()))
	return nex.NewRMCSuccess(conn.Settings, nex.ProtocolMatchmakeExtension, req.Method, req.CallID, out.Bytes())
}

// strikersListClubs answers MatchmakeExtension 0x49 (73, list clubs) with a LIST<club> of the
// caller's clubs (decoder 0x1f534, element = the same club struct as 55).
func strikersListClubs(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	owner := uint64(conn.PID)
	clubStore.Lock()
	clubs := clubStore.byOwner[owner]
	clubStore.Unlock()

	out := nex.NewStreamOut(conn.Settings)
	out.U32(uint32(len(clubs)))
	for _, club := range clubs {
		nex.WriteStructure(out, club)
	}
	fmt.Printf("[Strikers 0x6d] method 73 list-clubs owner=%d -> %d club(s) (%dB)\n", owner, len(clubs), len(out.Bytes()))
	return nex.NewRMCSuccess(conn.Settings, nex.ProtocolMatchmakeExtension, req.Method, req.CallID, out.Bytes())
}

// strikersClubRoster answers MatchmakeExtension 0x53 (83, club member roster) with a
// 1-element list = the owner. Member element (decoder 0x16090, byte-exact):
//   [u8 ver=0][u32 len=34] u16 role, u64 A=PID, u64 B, u64 C, u64 D=DateTime.
//
// The roster-detail screen GATES (scene update 0x5b3704 / 0x5b3964 early-returns -> "0/0"
// placeholder + freeze) until the member's nn::friends profile RESOLVES, THEN renders the card
// from the packed BUILD {u8 charId, u8 gear1-4}. Field mapping, PROVEN via emulator GetProfileList
// id-logging:
//   B(+0x18) = NSA account id: the game issues nn::friends GetProfileList([B]) to resolve the
//              member. With B=0 the request arrived as all-zero ids (req=100 nonzero=0) -> the
//              game keys the profile fetch on B. So B must be a nonzero, fillable id.
//   C(+0x20) = packed build {u8 charId, u8 gear1-4}: local name/stat render. C=ownerPID gave a
//              garbage loadout -> gear id out-of-range -> Core::Unknown (0x80010001). C=0 ->
//              charId 0 = Mario, valid.
// So B = the owner's id (nonzero -> our patched emulator GetProfileList fills + caches it -> the
// member resolves -> gate opens) and C = a valid build. The owner shown IS the local viewer.
func strikersClubRoster(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	owner := uint64(conn.PID)
	out := nex.NewStreamOut(conn.Settings)
	out.U32(1)                         // list count = 1 member
	out.U8(0)                          // member struct version
	out.U32(34)                        // content length (role+PID+B+C+DateTime = 2+8+8+8+8)
	out.U16(0)                         // role / slot index
	out.U64(owner)                     // A: member PID (owner)
	out.U64(owner)                     // B: NSA account id — game fetches GetProfileList([B]) on it
	out.U64(0)                         // C: packed build {charId=0 Mario, gear1-4=0} — valid loadout
	out.U64(nex.NowDateTime().Value()) // D: join / last-seen timestamp
	fmt.Printf("[Strikers 0x6d] method 83 club-roster -> 1 member (owner=%d, NSA=%d, build=0)\n", owner, owner)
	return nex.NewRMCSuccess(conn.Settings, nex.ProtocolMatchmakeExtension, req.Method, req.CallID, out.Bytes())
}

// strikersGetClub answers the single-club methods 56/58/59 (0x38/0x3a/0x3b — update/join/get
// ONE club) with the caller's REAL club struct (decoder 0x15970), not a generic zero-struct.
// Method 56 is "join club": returning a valid club lets the game enter it instead of
// overflowing on a mismatched envelope. The caller owns exactly one club here, so return it.
func strikersGetClub(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	owner := uint64(conn.PID)
	clubStore.Lock()
	clubs := clubStore.byOwner[owner]
	clubStore.Unlock()

	var club *strikersClub
	if len(clubs) > 0 {
		club = clubs[0]
	} else {
		club = newStrikersClub(owner, "Nextendo") // safety net: never send an empty/bad struct
	}
	out := nex.NewStreamOut(conn.Settings)
	nex.WriteStructure(out, club)
	fmt.Printf("[Strikers 0x6d] method %d get/join-club -> gid=%d owner=%d (%dB)\n", req.Method, club.ID, owner, len(out.Bytes()))
	return nex.NewRMCSuccess(conn.Settings, nex.ProtocolMatchmakeExtension, req.Method, req.CallID, out.Bytes())
}

// --- Persistence: clubs survive a server restart (redeploy) -------------------
// Only the identity (gid, owner, name) is saved to CLUB_STORE (default /data/clubs.json);
// every other field is rebuilt via newStrikersClub on load.

type savedClub struct {
	GID   uint32 `json:"gid"`
	Owner uint64 `json:"owner"`
	Name  string `json:"name"`
}

func clubStorePath() string { return envOr("CLUB_STORE", "/data/clubs.json") }

func saveClubs() {
	clubStore.Lock()
	var saved []savedClub
	for owner, clubs := range clubStore.byOwner {
		for _, c := range clubs {
			saved = append(saved, savedClub{GID: c.ID, Owner: owner, Name: c.Description})
		}
	}
	clubStore.Unlock()
	if data, err := json.Marshal(saved); err == nil {
		_ = os.WriteFile(clubStorePath(), data, 0o644)
	}
}

func loadClubs() {
	data, err := os.ReadFile(clubStorePath())
	if err != nil {
		return
	}
	var saved []savedClub
	if err := json.Unmarshal(data, &saved); err != nil {
		return
	}
	clubStore.Lock()
	var maxGID uint32
	for _, sc := range saved {
		c := newStrikersClub(sc.Owner, sc.Name)
		c.ID = sc.GID
		clubStore.byOwner[sc.Owner] = append(clubStore.byOwner[sc.Owner], c)
		if sc.GID > maxGID {
			maxGID = sc.GID
		}
	}
	clubStore.Unlock()
	if maxGID > atomic.LoadUint32(&strikersClubGID) {
		atomic.StoreUint32(&strikersClubGID, maxGID)
	}
	fmt.Printf("[Strikers] loaded %d club(s) from %s\n", len(saved), clubStorePath())
}
