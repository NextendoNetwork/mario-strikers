package main

import (
	"fmt"

	nex "github.com/NextendoNetwork/nextendo-nex"
)

// piaKeepalivePacketType is the undocumented PRUDP packet type 8 that Pia 5.x
// sends as a periodic keepalive on the secure stream (empty payload, NeedsAck,
// ~every 3s). The base PRUDP switch only knows types 0-4, so with no handler the
// keepalive goes un-ACKed and the client treats the connection as dead → soft-lock
// when entering online. ACKing it unblocks the online menu. Generic Pia behaviour
// (proven on SSBU); Strikers is a Pia title too. Harmless for MK8 (never sends type 8).
const piaKeepalivePacketType uint8 = 8

func setupPiaType8Keepalive(endpoint *nex.Endpoint) {
	endpoint.RegisterCustomPacketHandler(piaKeepalivePacketType, func(c *nex.Connection, p *nex.Packet) {
		if p.HasFlag(nex.FlagNeedACK) {
			c.SendAck(p)
		}
		fmt.Printf("[Strikers Type8] Pia keepalive seqID=%d -> ACK\n", p.PacketID)
	})
}
