package main

import (
	"encoding/json"
	"errors"
	"fmt"
	nex "github.com/NextendoNetwork/nextendo-nex"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

// Wire layouts verified against build 2e90f0815b1f92a1cbf013b3f4d358f41d729f4e:
// CreateClubParam 0x1671c; UpdateClubParam 0x1d2e4; Club 0x15970;
// CreatePlayerParam 0x169f4; UpdatePlayerParam 0x1d694; Player 0x1b930.
// Customization buffers are opaque game data and must survive updates/restarts intact.
type strikersClub struct {
	nex.Gathering
	Tags          []uint32
	JoinPolicy    uint32
	Customization []byte
	Name          string
	Message       string
	MemberCount   uint32
	Division      uint32
	Created       uint64
}

func (c *strikersClub) Levels() []nex.Level {
	return append(c.Gathering.Levels(), nex.Level{Version: 0, Save: func(o *nex.StreamOut) {
		writeAttributes(o, c.Tags)
		o.U32(c.JoinPolicy)
		o.Buffer(c.Customization)
		o.String(c.Name)
		o.String(c.Message)
		o.U32(c.MemberCount)
		o.U32(c.Division)
		o.U64(c.Created)
	}})
}

type strikersPlayer struct {
	PID           uint64
	Region        uint16
	ClubID        uint32
	Attributes    []uint32
	Role          uint32
	Customization []byte
	Flags         uint32
	Joined        uint64
}

func (p *strikersPlayer) Levels() []nex.Level {
	return []nex.Level{{Version: 0, Save: func(o *nex.StreamOut) {
		o.U64(p.PID)
		o.U16(p.Region)
		o.U32(p.ClubID)
		writeAttributes(o, p.Attributes)
		o.U32(p.Role)
		o.Buffer(p.Customization)
		o.U32(p.Flags)
		o.U64(p.Joined)
	}}}
}
func writeAttributes(o *nex.StreamOut, a []uint32) {
	nex.WriteList(o, a, func(o *nex.StreamOut, v uint32) { o.U32(v) })
}

type strikersState struct {
	Version int
	NextID  uint32
	Clubs   map[uint32]*strikersClub
	Players map[uint64]*strikersPlayer
}

var clubStore = struct {
	sync.Mutex
	State strikersState
}{State: emptyClubState()}

func emptyClubState() strikersState {
	return strikersState{Version: 1, NextID: 0x10000000, Clubs: map[uint32]*strikersClub{}, Players: map[uint64]*strikersPlayer{}}
}
func clubStorePath() string { return envOr("CLUB_STORE", "data/clubs.json") }

// Hold the store lock through persistence so concurrent writes cannot save stale snapshots.
// Publish success only after an atomic on-disk replacement; roll back failed writes.
func commitClubs(before []byte) error {
	data, err := json.MarshalIndent(clubStore.State, "", "  ")
	if err == nil {
		err = writeClubFile(clubStorePath(), data)
	}
	if err != nil {
		var restored strikersState
		if e := json.Unmarshal(before, &restored); e != nil {
			panic(e)
		}
		clubStore.State = restored
	}
	return err
}
func writeClubFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".clubs-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
func loadClubs() {
	path := clubStorePath()
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		panic(fmt.Errorf("read club store: %w", err))
	}
	clubStore.Lock()
	defer clubStore.Unlock()
	var state strikersState
	if err = json.Unmarshal(data, &state); err != nil || state.Version != 1 || state.Clubs == nil || state.Players == nil {
		// Never silently replace an unrecognized/legacy store with empty data.
		panic(fmt.Errorf("unsupported or invalid club store %s; preserve and migrate before starting", path))
	}
	clubStore.State = state
	fmt.Printf("[Strikers] loaded %d clubs and %d players from %s\n", len(state.Clubs), len(state.Players), path)
}

// Strict framing prevents truncated input from overwriting good club/player records.
func clubParam(body []byte, s *nex.Settings, maxVersion uint8) (*nex.StreamIn, uint8, error) {
	in := nex.NewStreamIn(body, s)
	v := in.U8()
	b := in.Buffer()
	if in.Err() != nil || in.Remaining() != 0 || v > maxVersion {
		return nil, v, fmt.Errorf("invalid parameter envelope (version %d, bytes %d)", v, len(body))
	}
	return nex.NewStreamIn(b, s), v, nil
}
func paramDone(in *nex.StreamIn) error {
	if in.Err() != nil {
		return in.Err()
	}
	if in.Remaining() != 0 {
		return fmt.Errorf("unexpected parameter tail: %d", in.Remaining())
	}
	return nil
}
func readAttributes(in *nex.StreamIn) []uint32 {
	return nex.ReadList(in, func(i *nex.StreamIn) uint32 { return i.U32() })
}
func readAttributeChanges(in *nex.StreamIn) []string {
	return nex.ReadList(in, func(i *nex.StreamIn) string { return i.String() })
}

// Update parameters carry decimal strings; an empty string means leave this index alone
// (game glue 0x64c50c/0x64d2a0), not replace it with zero.
func mergeAttributes(old []uint32, changes []string) ([]uint32, error) {
	a := append([]uint32(nil), old...)
	if len(changes) > 64 {
		return nil, errors.New("too many attributes")
	}
	for i, s := range changes {
		if s == "" {
			continue
		}
		n, e := strconv.ParseUint(s, 10, 32)
		if e != nil {
			return nil, e
		}
		for len(a) <= i {
			a = append(a, 0)
		}
		a[i] = uint32(n)
	}
	return a, nil
}
func clubSuccess(c *nex.Connection, r *nex.RMCMessage, o *nex.StreamOut) *nex.RMCMessage {
	return nex.NewRMCSuccess(c.Settings, nex.ProtocolMatchmakeExtension, r.Method, r.CallID, o.Bytes())
}
func clubFailure(c *nex.Connection, r *nex.RMCMessage, err error) *nex.RMCMessage {
	fmt.Printf("[Strikers clubs] method=%d pid=%d failed: %v\n", r.Method, c.PID, err)
	return nex.NewRMCError(c.Settings, nex.ProtocolMatchmakeExtension, r.CallID, 0x80010004)
}
func playerFor(pid uint64) *strikersPlayer {
	p := clubStore.State.Players[pid]
	if p == nil {
		p = &strikersPlayer{PID: pid, Attributes: make([]uint32, 10), Customization: []byte{0, 0, 0, 0, 1}}
		clubStore.State.Players[pid] = p
	}
	return p
}
func refreshCounts() {
	for _, c := range clubStore.State.Clubs {
		c.MemberCount = 0
	}
	for _, p := range clubStore.State.Players {
		if c := clubStore.State.Clubs[p.ClubID]; c != nil {
			c.MemberCount++
		}
	}
}

func strikersCreateClub(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	in, v, err := clubParam(req.Body, conn.Settings, 1)
	if err != nil {
		return clubFailure(conn, req, err)
	}
	tags := readAttributes(in)
	policy := in.U32()
	custom := append([]byte(nil), in.Buffer()...)
	name := in.String()
	message := in.String()
	if v >= 1 {
		in.U16()
	} // language, retained in original request schema, not Club v0
	if err = paramDone(in); err != nil {
		return clubFailure(conn, req, err)
	}
	if len(tags) < 4 || len(tags) > 64 || len(custom) < 10 || len(custom) > 4096 || name == "" || len(name) > 256 || policy < 1 || policy > 3 {
		return clubFailure(conn, req, errors.New("invalid club settings"))
	}
	clubStore.Lock()
	defer clubStore.Unlock()
	before, _ := json.Marshal(clubStore.State)
	p := playerFor(uint64(conn.PID))
	if p.ClubID != 0 {
		return clubFailure(conn, req, errors.New("player already belongs to a club"))
	}
	clubStore.State.NextID++
	now := nex.NowDateTime().Value()
	c := &strikersClub{Tags: tags, JoinPolicy: policy, Customization: custom, Name: name, Message: message, Created: now}
	c.ID = clubStore.State.NextID
	c.OwnerPID = uint64(conn.PID)
	c.HostPID = c.OwnerPID
	c.MinParticipants = 1
	c.MaxParticipants = 20
	c.ParticipationPolicy = 98
	c.State = 1
	c.Description = name
	clubStore.State.Clubs[c.ID] = c
	p.ClubID = c.ID
	p.Joined = now
	refreshCounts()
	if err = commitClubs(before); err != nil {
		return clubFailure(conn, req, err)
	}
	out := nex.NewStreamOut(conn.Settings)
	nex.WriteStructure(out, c)
	fmt.Printf("[Strikers clubs] created gid=%d owner=%d members=%d\n", c.ID, c.OwnerPID, c.MemberCount)
	return clubSuccess(conn, req, out)
}
func strikersGetClub(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	var id uint32
	var changes []string
	var policy uint32
	var custom []byte
	var name, message string
	if req.Method == 58 {
		in, _, err := clubParam(req.Body, conn.Settings, 0)
		if err != nil {
			return clubFailure(conn, req, err)
		}
		id = in.U32()
		changes = readAttributeChanges(in)
		policy = in.U32()
		custom = append([]byte(nil), in.Buffer()...)
		name = in.String()
		message = in.String()
		if err = paramDone(in); err != nil {
			return clubFailure(conn, req, err)
		}
		if policy > 3 || (len(custom) > 0 && len(custom) < 10) || len(custom) > 4096 || len(name) > 256 {
			return clubFailure(conn, req, errors.New("invalid club update"))
		}
	} else {
		in := nex.NewStreamIn(req.Body, conn.Settings)
		id = in.U32()
		if err := paramDone(in); err != nil {
			return clubFailure(conn, req, err)
		}
	}
	clubStore.Lock()
	defer clubStore.Unlock()
	c := clubStore.State.Clubs[id]
	if c == nil {
		return clubFailure(conn, req, errors.New("club not found"))
	}
	if req.Method == 58 || req.Method == 56 {
		before, _ := json.Marshal(clubStore.State)
		if req.Method == 58 {
			if c.OwnerPID != uint64(conn.PID) {
				return clubFailure(conn, req, errors.New("only the club owner may edit settings"))
			}
			tags, err := mergeAttributes(c.Tags, changes)
			if err != nil {
				return clubFailure(conn, req, err)
			}
			c.Tags = tags
			if policy != 0 {
				c.JoinPolicy = policy
			}
			if len(custom) > 0 {
				c.Customization = custom
			}
			if name != "" {
				c.Name = name
				c.Description = name
			}
			if message != "" {
				c.Message = message
			}
		} else {
			p := playerFor(uint64(conn.PID))
			if p.ClubID != id {
				if p.ClubID != 0 || c.JoinPolicy != 1 || c.MemberCount >= uint32(c.MaxParticipants) {
					return clubFailure(conn, req, errors.New("club cannot be joined directly"))
				}
				p.ClubID = id
				p.Joined = nex.NowDateTime().Value()
				refreshCounts()
			}
		}
		if err := commitClubs(before); err != nil {
			return clubFailure(conn, req, err)
		}
	}
	out := nex.NewStreamOut(conn.Settings)
	nex.WriteStructure(out, c)
	return clubSuccess(conn, req, out)
}
func strikersListClubs(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	clubStore.Lock()
	defer clubStore.Unlock()
	ids := make([]int, 0, len(clubStore.State.Clubs))
	for id := range clubStore.State.Clubs {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	if len(ids) > 20 {
		ids = ids[:20]
	}
	out := nex.NewStreamOut(conn.Settings)
	out.U32(uint32(len(ids)))
	for _, id := range ids {
		nex.WriteStructure(out, clubStore.State.Clubs[uint32(id)])
	}
	return clubSuccess(conn, req, out)
}
func strikersPlayerRequest(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	max := uint8(2)
	if req.Method == 65 {
		max = 1
	}
	in, v, err := clubParam(req.Body, conn.Settings, max)
	if err != nil {
		return clubFailure(conn, req, err)
	}
	region := in.U16()
	var attrs []uint32
	var changes []string
	if req.Method == 62 {
		attrs = readAttributes(in)
	} else {
		changes = readAttributeChanges(in)
	}
	custom := append([]byte(nil), in.Buffer()...)
	if req.Method == 62 && v >= 1 {
		_ = in.String()
	}
	if (req.Method == 62 && v >= 2) || (req.Method == 65 && v >= 1) {
		in.U16()
	}
	if err = paramDone(in); err != nil {
		return clubFailure(conn, req, err)
	}
	if len(attrs) > 64 || len(custom) > 4096 || (len(custom) > 0 && len(custom) < 5) {
		return clubFailure(conn, req, errors.New("invalid player settings"))
	}
	clubStore.Lock()
	defer clubStore.Unlock()
	before, _ := json.Marshal(clubStore.State)
	p := clubStore.State.Players[uint64(conn.PID)]
	fresh := p == nil
	p = playerFor(uint64(conn.PID))
	if fresh {
		p.Region = region
		if req.Method == 62 {
			p.Attributes = attrs
		}
		if len(custom) > 0 {
			p.Customization = custom
		}
	}
	if req.Method == 65 {
		merged, e := mergeAttributes(p.Attributes, changes)
		if e != nil {
			var restored strikersState
			json.Unmarshal(before, &restored)
			clubStore.State = restored
			return clubFailure(conn, req, e)
		}
		p.Attributes = merged
		if region != 0 {
			p.Region = region
		}
		if len(custom) > 0 {
			p.Customization = custom
		}
	}
	if err = commitClubs(before); err != nil {
		return clubFailure(conn, req, err)
	}
	out := nex.NewStreamOut(conn.Settings)
	nex.WriteStructure(out, p)
	fmt.Printf("[Strikers clubs] player method=%d pid=%d club=%d\n", req.Method, p.PID, p.ClubID)
	return clubSuccess(conn, req, out)
}
func strikersClubPlayers(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	in := nex.NewStreamIn(req.Body, conn.Settings)
	id := in.U32()
	if err := paramDone(in); err != nil {
		return clubFailure(conn, req, err)
	}
	clubStore.Lock()
	defer clubStore.Unlock()
	members := []*strikersPlayer{}
	for _, p := range clubStore.State.Players {
		if p.ClubID == id && id != 0 {
			members = append(members, p)
		}
	}
	sort.Slice(members, func(i, j int) bool { return members[i].PID < members[j].PID })
	out := nex.NewStreamOut(conn.Settings)
	out.U32(uint32(len(members)))
	for _, p := range members {
		nex.WriteStructure(out, p)
	}
	return clubSuccess(conn, req, out)
}

// GetClubUpdate currently uses the member-update envelope expected by decoder 0x16090.
// Membership comes from the stored player record, never from the viewing caller alone.
func strikersClubRoster(conn *nex.Connection, req *nex.RMCMessage) *nex.RMCMessage {
	clubStore.Lock()
	defer clubStore.Unlock()
	out := nex.NewStreamOut(conn.Settings)
	p := clubStore.State.Players[uint64(conn.PID)]
	if p == nil || p.ClubID == 0 {
		out.U32(0)
		return clubSuccess(conn, req, out)
	}
	members := []*strikersPlayer{}
	for _, m := range clubStore.State.Players {
		if m.ClubID == p.ClubID {
			members = append(members, m)
		}
	}
	sort.Slice(members, func(i, j int) bool { return members[i].PID < members[j].PID })
	out.U32(uint32(len(members)))
	for _, m := range members {
		out.U8(0)
		out.U32(34)
		out.U16(0)
		out.U64(m.PID)
		out.U64(m.PID)
		out.U64(0) // existing valid default loadout; player customization is returned by GetClubPlayer
		out.U64(m.Joined)
	}
	return clubSuccess(conn, req, out)
}

