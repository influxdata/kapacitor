package kafka

import (
	"bufio"
	"bytes"
)

type memberGroupMetadata struct {
	// MemberID assigned by the group coordinator or null if joining for the
	// first time.
	MemberID string
	Metadata groupMetadata
}

type groupMetadata struct {
	Version  int16
	Topics   []string
	UserData []byte
}

func (t groupMetadata) size() int32 {
	return sizeofInt16(t.Version) +
		sizeofStringArray(t.Topics) +
		sizeofBytes(t.UserData)
}

func (t groupMetadata) writeTo(w *bufio.Writer) {
	writeInt16(w, t.Version)
	writeStringArray(w, t.Topics)
	writeBytes(w, t.UserData)
}

func (t groupMetadata) bytes() []byte {
	buf := bytes.NewBuffer(nil)
	w := bufio.NewWriter(buf)
	t.writeTo(w)
	w.Flush()
	return buf.Bytes()
}

func (t *groupMetadata) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt16(r, size, &t.Version); err != nil {
		return
	}
	if remain, err = readStringArray(r, remain, &t.Topics); err != nil {
		return
	}
	if remain, err = readBytes(r, remain, &t.UserData); err != nil {
		return
	}
	return
}

type joinGroupRequestGroupProtocolV2 struct {
	ProtocolName     string
	ProtocolMetadata []byte
}

func (t joinGroupRequestGroupProtocolV2) size() int32 {
	return sizeofString(t.ProtocolName) +
		sizeofBytes(t.ProtocolMetadata)
}

func (t joinGroupRequestGroupProtocolV2) writeTo(w *bufio.Writer) {
	writeString(w, t.ProtocolName)
	writeBytes(w, t.ProtocolMetadata)
}

type joinGroupRequestV2 struct {
	// GroupID holds the unique group identifier
	GroupID string

	// SessionTimeout holds the coordinator considers the consumer dead if it
	// receives no heartbeat after this timeout in ms.
	SessionTimeout int32

	// RebalanceTimeout holds the maximum time that the coordinator will wait
	// for each member to rejoin when rebalancing the group in ms
	RebalanceTimeout int32

	// MemberID assigned by the group coordinator or the zero string if joining
	// for the first time.
	MemberID string

	// ProtocolType holds the unique name for class of protocols implemented by group
	ProtocolType string

	// GroupProtocols holds the list of protocols that the member supports
	GroupProtocols []joinGroupRequestGroupProtocolV2
}

func (t joinGroupRequestV2) size() int32 {
	return sizeofString(t.GroupID) +
		sizeofInt32(t.SessionTimeout) +
		sizeofInt32(t.RebalanceTimeout) +
		sizeofString(t.MemberID) +
		sizeofString(t.ProtocolType) +
		sizeofArray(len(t.GroupProtocols), func(i int) int32 { return t.GroupProtocols[i].size() })
}

func (t joinGroupRequestV2) writeTo(w *bufio.Writer) {
	writeString(w, t.GroupID)
	writeInt32(w, t.SessionTimeout)
	writeInt32(w, t.RebalanceTimeout)
	writeString(w, t.MemberID)
	writeString(w, t.ProtocolType)
	writeArray(w, len(t.GroupProtocols), func(i int) { t.GroupProtocols[i].writeTo(w) })
}

type joinGroupResponseMemberV2 struct {
	// MemberID assigned by the group coordinator
	MemberID       string
	MemberMetadata []byte
}

func (t joinGroupResponseMemberV2) size() int32 {
	return sizeofString(t.MemberID) +
		sizeofBytes(t.MemberMetadata)
}

func (t joinGroupResponseMemberV2) writeTo(w *bufio.Writer) {
	writeString(w, t.MemberID)
	writeBytes(w, t.MemberMetadata)
}

func (t *joinGroupResponseMemberV2) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readString(r, size, &t.MemberID); err != nil {
		return
	}
	if remain, err = readBytes(r, remain, &t.MemberMetadata); err != nil {
		return
	}
	return
}

type joinGroupResponseV2 struct {
	// ThrottleTimeMS holds the duration in milliseconds for which the request
	// was throttled due to quota violation (Zero if the request did not violate
	// any quota)
	ThrottleTimeMS int32

	// ErrorCode holds response error code
	ErrorCode int16

	// GenerationID holds the generation of the group.
	GenerationID int32

	// GroupProtocol holds the group protocol selected by the coordinator
	GroupProtocol string

	// LeaderID holds the leader of the group
	LeaderID string

	// MemberID assigned by the group coordinator
	MemberID string
	Members  []joinGroupResponseMemberV2
}

func (t joinGroupResponseV2) size() int32 {
	return sizeofInt32(t.ThrottleTimeMS) +
		sizeofInt16(t.ErrorCode) +
		sizeofInt32(t.GenerationID) +
		sizeofString(t.GroupProtocol) +
		sizeofString(t.LeaderID) +
		sizeofString(t.MemberID) +
		sizeofArray(len(t.MemberID), func(i int) int32 { return t.Members[i].size() })
}

func (t joinGroupResponseV2) writeTo(w *bufio.Writer) {
	writeInt32(w, t.ThrottleTimeMS)
	writeInt16(w, t.ErrorCode)
	writeInt32(w, t.GenerationID)
	writeString(w, t.GroupProtocol)
	writeString(w, t.LeaderID)
	writeString(w, t.MemberID)
	writeArray(w, len(t.Members), func(i int) { t.Members[i].writeTo(w) })
}

func (t *joinGroupResponseV2) readFrom(r *bufio.Reader, size int) (remain int, err error) {
	if remain, err = readInt32(r, size, &t.ThrottleTimeMS); err != nil {
		return
	}
	if remain, err = readInt16(r, remain, &t.ErrorCode); err != nil {
		return
	}
	if remain, err = readInt32(r, remain, &t.GenerationID); err != nil {
		return
	}
	if remain, err = readString(r, remain, &t.GroupProtocol); err != nil {
		return
	}
	if remain, err = readString(r, remain, &t.LeaderID); err != nil {
		return
	}
	if remain, err = readString(r, remain, &t.MemberID); err != nil {
		return
	}

	fn := func(r *bufio.Reader, size int) (fnRemain int, fnErr error) {
		var item joinGroupResponseMemberV2
		if fnRemain, fnErr = (&item).readFrom(r, size); fnErr != nil {
			return
		}
		t.Members = append(t.Members, item)
		return
	}
	if remain, err = readArrayWith(r, remain, fn); err != nil {
		return
	}

	return
}
