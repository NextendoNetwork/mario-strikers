package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	nex "github.com/NextendoNetwork/nextendo-nex"
	"os"
	"path/filepath"
	"testing"
)

func fixture(t *testing.T) *nex.Connection {
	t.Helper()
	clubStore.State = emptyClubState()
	t.Setenv("CLUB_STORE", filepath.Join(t.TempDir(), "nested", "clubs.json"))
	return &nex.Connection{PID: 1234, Settings: nex.NewSwitchSettings("test", 40600)}
}
func param(c *nex.Connection, v uint8, fn func(*nex.StreamOut)) []byte {
	f := nex.NewStreamOut(c.Settings)
	fn(f)
	o := nex.NewStreamOut(c.Settings)
	o.U8(v)
	o.Buffer(f.Bytes())
	return o.Bytes()
}
func request(method uint32, body []byte) *nex.RMCMessage {
	return &nex.RMCMessage{Method: method, CallID: 7, Body: body}
}
func mustOK(t *testing.T, r *nex.RMCMessage) {
	t.Helper()
	if r == nil || r.IsError {
		t.Fatalf("RPC failed: %+v", r)
	}
}
func createPlayerFixture(t *testing.T, c *nex.Connection) {
	t.Helper()
	b := param(c, 2, func(o *nex.StreamOut) {
		o.U16(1)
		writeAttributes(o, make([]uint32, 10))
		o.Buffer([]byte{0, 1, 2, 3, 1})
		o.String("")
		o.U16(0)
	})
	if len(b) != 65 {
		t.Fatal("GetOrCreatePlayer wire length must match live 65-byte request")
	}
	mustOK(t, strikersPlayerRequest(c, request(62, b)))
}
func createClubFixture(t *testing.T, c *nex.Connection) uint32 {
	t.Helper()
	b := param(c, 1, func(o *nex.StreamOut) {
		writeAttributes(o, []uint32{0, 2, 3, 4, 0, 0})
		o.U32(1)
		o.Buffer([]byte{1, 2, 3, 0, 4, 5, 6, 7, 0, 8})
		o.String("Collectings")
		o.String("")
		o.U16(0)
	})
	r := strikersCreateClub(c, request(55, b))
	mustOK(t, r)
	return binary.LittleEndian.Uint32(r.Body[5:9])
}
func TestClubSettingsMembershipAndRestart(t *testing.T) {
	c := fixture(t)
	createPlayerFixture(t, c)
	gid := createClubFixture(t, c)
	custom := []byte{2, 3, 4, 0, 5, 6, 7, 8, 0, 9}
	update := param(c, 0, func(o *nex.StreamOut) {
		o.U32(gid)
		nex.WriteList(o, []string{"", "4", "", "7", "", ""}, func(o *nex.StreamOut, s string) { o.String(s) })
		o.U32(0)
		o.Buffer(custom)
		o.String("")
		o.String("")
	})
	mustOK(t, strikersGetClub(c, request(58, update)))
	// Reopening submits creation defaults again. Existing player/club state must win.
	createPlayerFixture(t, c)
	playerUpdate := param(c, 1, func(o *nex.StreamOut) {
		o.U16(0)
		nex.WriteList(o, []string{"", "9"}, func(o *nex.StreamOut, s string) { o.String(s) })
		o.Buffer([]byte{3, 2, 1, 0, 4})
		o.U16(0)
	})
	r := strikersPlayerRequest(c, request(65, playerUpdate))
	mustOK(t, r)
	// Independent decoder offsets: PID (8), region (2), club ID (4), after 5-byte header.
	if binary.LittleEndian.Uint64(r.Body[5:13]) != 1234 || binary.LittleEndian.Uint32(r.Body[15:19]) != gid {
		t.Fatal("player identity/membership missing on wire")
	}
	clubStore.State = emptyClubState()
	loadClubs()
	p := clubStore.State.Players[1234]
	club := clubStore.State.Clubs[gid]
	if p.ClubID != gid || p.Attributes[1] != 9 || !bytes.Equal(p.Customization, []byte{3, 2, 1, 0, 4}) {
		t.Fatalf("player did not survive reload: %+v", p)
	}
	if club.Name != "Collectings" || club.Description != "Collectings" || club.MemberCount != 1 || !bytes.Equal(club.Customization, custom) || club.Tags[1] != 4 || club.Tags[2] != 3 || club.Tags[3] != 7 {
		t.Fatalf("club did not survive reload: %+v", club)
	}
	get := make([]byte, 4)
	binary.LittleEndian.PutUint32(get, gid)
	mustOK(t, strikersGetClub(c, request(59, get)))
	r = strikersClubPlayers(c, request(63, get))
	mustOK(t, r)
	if binary.LittleEndian.Uint32(r.Body[:4]) != 1 {
		t.Fatal("missing owner in club players")
	}
	other := &nex.Connection{PID: 5678, Settings: c.Settings}
	r = strikersListClubs(other, request(73, nil))
	mustOK(t, r)
	if binary.LittleEndian.Uint32(r.Body[:4]) != 1 {
		t.Fatal("recommendations hidden from other player")
	}
	mustOK(t, strikersGetClub(other, request(56, get)))
	if clubStore.State.Players[5678].ClubID != gid || clubStore.State.Clubs[gid].MemberCount != 2 {
		t.Fatal("join failed to persist membership/count")
	}
	if !strikersGetClub(other, request(58, update)).IsError {
		t.Fatal("nonowner edited club")
	}
}
func TestRejectedInputAndFailedSaveDoNotAlterState(t *testing.T) {
	c := fixture(t)
	createPlayerFixture(t, c)
	createClubFixture(t, c)
	before, _ := json.Marshal(clubStore.State)
	for _, b := range [][]byte{nil, {1, 255, 255, 255, 127}, {2, 0, 0, 0, 0}} {
		if !strikersCreateClub(c, request(55, b)).IsError {
			t.Fatal("malformed create succeeded")
		}
	}
	after, _ := json.Marshal(clubStore.State)
	if !bytes.Equal(before, after) {
		t.Fatal("malformed input changed state")
	}
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLUB_STORE", filepath.Join(file, "clubs.json"))
	update := param(c, 1, func(o *nex.StreamOut) { o.U16(0); o.U32(0); o.Buffer([]byte{1, 1, 1, 1, 1}); o.U16(0) })
	if !strikersPlayerRequest(c, request(65, update)).IsError {
		t.Fatal("failed persistence returned success")
	}
	after, _ = json.Marshal(clubStore.State)
	if !bytes.Equal(before, after) {
		t.Fatal("failed persistence changed state")
	}
}
func TestTruncatedClubCreate(t *testing.T) {
	c := fixture(t)
	b := param(c, 1, func(o *nex.StreamOut) {
		writeAttributes(o, make([]uint32, 6))
		o.U32(1)
		o.Buffer(make([]byte, 10))
		o.String("Collectings")
		o.String("")
		o.U16(0)
	})
	for n := 0; n < len(b); n++ {
		truncated := append([]byte(nil), b[:n]...)
		if n >= 5 {
			binary.LittleEndian.PutUint32(truncated[1:5], uint32(n-5))
		}
		if !strikersCreateClub(c, request(55, truncated)).IsError {
			t.Fatalf("accepted truncation %d", n)
		}
	}
	if len(clubStore.State.Clubs) != 0 {
		t.Fatal("truncated input created clubs")
	}
}
