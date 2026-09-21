package clusters

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	CLUSTERMSG_TYPE_PING = 0
	CLUSTERMSG_TYPE_PONG = 1

	CLUSTERMSG_HEADER_SIZE = 4 + // Sig
		4 + // totlen
		2 + // Type
		2 + // port
		40 + // sender
		2 + // cport
		2 + // flags
		2 + // State
		SLOT_WORDS*8 // slots bitmap
)

type clusterMsg struct {
	Sig    [4]byte
	Type   uint16
	totLen uint32
	count  uint16
	port   uint16
	sender string
	cport  uint16
	flags  uint16
	State  int
	slots  [SLOT_WORDS]uint64
	Gossip []*clusterMsgDataGossip
}

func (m *clusterMsg) GetSignature() [4]byte              { return m.Sig }
func (m *clusterMsg) SetSignature(sig [4]byte)           { m.Sig = sig }
func (m *clusterMsg) GetType() uint16                    { return m.Type }
func (m *clusterMsg) SetType(messageType uint16)         { m.Type = messageType }
func (m *clusterMsg) GetTotalLength() uint32             { return m.totLen }
func (m *clusterMsg) SetTotalLength(length uint32)       { m.totLen = length }
func (m *clusterMsg) SetCount(count uint16)              { m.count = count }
func (m *clusterMsg) GetClientPort() uint16              { return m.port }
func (m *clusterMsg) SetClientPort(port uint16)          { m.port = port }
func (m *clusterMsg) GetSender() string                  { return m.sender }
func (m *clusterMsg) SetSender(sender string)            { m.sender = sender }
func (m *clusterMsg) GetClusterBusPort() uint16          { return m.cport }
func (m *clusterMsg) SetClusterBusPort(port uint16)      { m.cport = port }
func (m *clusterMsg) GetFlags() uint16                   { return m.flags }
func (m *clusterMsg) SetFlags(flags uint16)              { m.flags = flags }
func (m *clusterMsg) GetState() int                      { return m.State }
func (m *clusterMsg) SetState(state int)                 { m.State = state }
func (m *clusterMsg) GetSlots() [SLOT_WORDS]uint64       { return m.slots }
func (m *clusterMsg) SetSlots(slots [SLOT_WORDS]uint64)  { m.slots = slots }
func (m *clusterMsg) GetGossip() []*clusterMsgDataGossip { return m.Gossip }
func (m *clusterMsg) AddGossip(gossip *clusterMsgDataGossip) {
	m.Gossip = append(m.Gossip, gossip)
}

func clusterMsgBuildHdr(messageType int) *clusterMsg {
	self := serverState.GetSelf()
	snapshot := self.Snapshot()

	hdr := &clusterMsg{}
	hdr.SetSignature([4]byte{'R', 'C', 'm', 'b'})
	hdr.SetType(uint16(messageType))
	hdr.SetSender(snapshot.Name)
	hdr.SetClusterBusPort(uint16(snapshot.ClusterBusPort))
	hdr.SetFlags(uint16(snapshot.Flags))
	hdr.SetState(serverState.GetState())
	hdr.SetClientPort(uint16(snapshot.ClientPort))
	hdr.SetSlots(snapshot.OwnedSlots)
	return hdr
}

func deserializeClusterMsg(buf []byte) (*clusterMsg, error) {
	if len(buf) < CLUSTERMSG_HEADER_SIZE {
		return nil, fmt.Errorf("cluster message too short: %d bytes", len(buf))
	}

	msg := &clusterMsg{}
	offset := 0

	copy(msg.Sig[:], buf[offset:offset+4])
	offset += 4
	if string(msg.Sig[:]) != "RCmb" {
		return nil, fmt.Errorf("invalid cluster message signature: %q", msg.Sig)
	}

	msg.totLen = binary.BigEndian.Uint32(buf[offset : offset+4])
	offset += 4
	if int(msg.totLen) != len(buf) {
		return nil, fmt.Errorf("cluster message length mismatch: header=%d actual=%d", msg.totLen, len(buf))
	}

	msg.Type = binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2
	msg.port = binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2
	msg.sender = string(bytes.TrimRight(buf[offset:offset+40], "\x00"))
	offset += 40
	msg.cport = binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2
	msg.flags = binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2
	msg.State = int(binary.BigEndian.Uint16(buf[offset : offset+2]))
	offset += 2

	for i := range msg.slots {
		msg.slots[i] = binary.BigEndian.Uint64(buf[offset : offset+8])
		offset += 8
	}

	remaining := len(buf) - offset
	if remaining%CLUSTERMSG_GOSSIP_SIZE != 0 {
		return nil, fmt.Errorf("invalid gossip payload size: %d bytes", remaining)
	}

	gossipCount := remaining / CLUSTERMSG_GOSSIP_SIZE
	msg.count = uint16(gossipCount)
	msg.Gossip = make([]*clusterMsgDataGossip, 0, gossipCount)
	for i := 0; i < gossipCount; i++ {
		gossip := &clusterMsgDataGossip{}
		gossip.SetNodeName(string(bytes.TrimRight(buf[offset:offset+40], "\x00")))
		offset += 40
		gossip.SetPingSent(binary.BigEndian.Uint32(buf[offset : offset+4]))
		offset += 4
		gossip.SetPongReceived(binary.BigEndian.Uint32(buf[offset : offset+4]))
		offset += 4
		gossip.SetClientPort(binary.BigEndian.Uint16(buf[offset : offset+2]))
		offset += 2
		gossip.SetClusterBusPort(binary.BigEndian.Uint16(buf[offset : offset+2]))
		offset += 2
		gossip.SetFlags(binary.BigEndian.Uint16(buf[offset : offset+2]))
		offset += 2
		msg.Gossip = append(msg.Gossip, gossip)
	}

	return msg, nil
}

func serializeClusterMsg(msg *clusterMsg) []byte {
	buf := make([]byte, 0, msg.totLen)
	buf = append(buf, msg.Sig[:]...)
	buf = binary.BigEndian.AppendUint32(buf, msg.totLen)
	buf = binary.BigEndian.AppendUint16(buf, msg.Type)
	buf = binary.BigEndian.AppendUint16(buf, msg.port)

	sender := make([]byte, 40)
	copy(sender, []byte(msg.sender))
	buf = append(buf, sender...)
	buf = binary.BigEndian.AppendUint16(buf, msg.cport)
	buf = binary.BigEndian.AppendUint16(buf, msg.flags)
	buf = binary.BigEndian.AppendUint16(buf, uint16(msg.State))
	for _, word := range msg.slots {
		buf = binary.BigEndian.AppendUint64(buf, word)
	}

	for _, gossip := range msg.Gossip {
		nodeName := make([]byte, 40)
		copy(nodeName, []byte(gossip.GetNodeName()))
		buf = append(buf, nodeName...)
		buf = binary.BigEndian.AppendUint32(buf, gossip.GetPingSent())
		buf = binary.BigEndian.AppendUint32(buf, gossip.GetPongReceived())
		buf = binary.BigEndian.AppendUint16(buf, gossip.GetClientPort())
		buf = binary.BigEndian.AppendUint16(buf, gossip.GetClusterBusPort())
		buf = binary.BigEndian.AppendUint16(buf, gossip.GetFlags())
	}

	if len(buf) != int(msg.totLen) {
		panic(fmt.Sprintf("cluster message size mismatch: encoded=%d expected=%d", len(buf), msg.totLen))
	}
	return buf
}
