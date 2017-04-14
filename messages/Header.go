// automatically generated, do not modify

package messages

import (
	flatbuffers "github.com/google/flatbuffers/go"
)
type Header struct {
	_tab flatbuffers.Table
}

func (rcv *Header) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Header) Key() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *Header) Value() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func HeaderStart(builder *flatbuffers.Builder) { builder.StartObject(2) }
func HeaderAddKey(builder *flatbuffers.Builder, key flatbuffers.UOffsetT) { builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(key), 0) }
func HeaderAddValue(builder *flatbuffers.Builder, value flatbuffers.UOffsetT) { builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(value), 0) }
func HeaderEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT { return builder.EndObject() }
