package ipc

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"testing"
)

func TestProtocolAllMessagesRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		msg  any
	}{
		{
			name: "HelloReq",
			msg:  &HelloReq{MajorVersion: ProtocolMajorVersion, ClientName: "xift-cli"},
		},
		{
			name: "HelloAck",
			msg:  &HelloAck{MajorVersion: ProtocolMajorVersion, DaemonPID: 42424},
		},
		{
			name: "QueryStartReq",
			msg: &QueryStartReq{
				SessionID: 99,
				Text:      "report 2026 ext:pdf !draft",
				Sort:      2,
				Desc:      true,
				PathSep:   '\\',
			},
		},
		{
			name: "QueryResultMeta",
			msg: &QueryResultMeta{
				SessionID:   99,
				Total:       3_000_000,
				Retained:    1_000_000,
				CostMicros:  4321,
				Incremental: true,
				Truncated:   true,
				Warnings:    []string{"content: reserved", "second warning"},
			},
		},
		{
			name: "FetchViewportReq",
			msg:  &FetchViewportReq{SessionID: 99, Offset: 9000, Limit: 60},
		},
		{
			name: "ViewportRows",
			msg: &ViewportRows{
				SessionID: 99,
				Offset:    9000,
				Rows: []ViewportRow{
					{
						NodeID:  12345,
						Flags:   0x0005,
						ExtID:   7,
						VolID:   1,
						Size:    65536,
						ModTime: 1732694400000000000,
						Score:   420.5,
						Path:    `D:\code\report_2026.pdf`,
						Name:    "report_2026.pdf",
						Highlights: []HighlightRange{
							{Start: 0, End: 6},
							{Start: 7, End: 11},
						},
					},
					{
						NodeID:  0,
						Flags:   RowFlagFromFoldedScan,
						Size:    128,
						ModTime: -1,
						Score:   100.25,
						Path:    "/tmp/node_modules/lodash/index.js",
						Name:    "index.js",
					},
				},
			},
		},
		{
			name: "CancelReq",
			msg:  &CancelReq{SessionID: 99},
		},
		{
			name: "StatusReq",
			msg:  &StatusReq{},
		},
		{
			name: "StatusReply",
			msg: &StatusReply{
				ProtocolVersion: ProtocolMajorVersion,
				LiveCount:       1_234_567,
				ActiveSessions:  3,
				RejectedConns:   7,
			},
		},
		{
			name: "ErrorResp",
			msg: &ErrorResp{
				Code:    ErrStaleSession,
				Message: "session 41 superseded",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msgType, payload, err := EncodeMessage(tc.msg)
			if err != nil {
				t.Fatalf("EncodeMessage: %v", err)
			}
			var buf bytes.Buffer
			if err := WriteFrame(&buf, msgType, 777, payload); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
			frame, err := ReadFrame(&buf)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if frame.Version != ProtocolMajorVersion || frame.Type != msgType || frame.ReqID != 777 {
				t.Fatalf("header mismatch: %+v", frame)
			}
			got, err := DecodeMessage(frame.Type, frame.Payload)
			if err != nil {
				t.Fatalf("DecodeMessage: %v", err)
			}
			if !reflect.DeepEqual(got, tc.msg) {
				t.Fatalf("roundtrip mismatch:\n got: %#v\nwant: %#v", got, tc.msg)
			}
		})
	}
}

// TestQueryResultMetaBudgetTruncation verifies the existing Truncated bit carries
// budget truncation with Total as the lower bound, and that the frame layout
// stays byte-stable so older clients keep decoding new daemons' frames.
func TestQueryResultMetaBudgetTruncation(t *testing.T) {
	msg := &QueryResultMeta{SessionID: 7, Total: 8192, Retained: 100, Truncated: true}
	msgType, payload, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	if msgType != MsgQueryResultMeta {
		t.Fatalf("type = %v, want %v", msgType, MsgQueryResultMeta)
	}
	// sid(8) + total(8) + retained(4) + cost(4) + flags(2) + warning count(1).
	if want := 27; len(payload) != want {
		t.Fatalf("payload = %d bytes, want %d (no new field may be appended)", len(payload), want)
	}
	got, err := DecodeMessage(msgType, payload)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	meta, ok := got.(*QueryResultMeta)
	if !ok || !meta.Truncated || meta.Total != 8192 {
		t.Fatalf("decoded = %#v, want truncated meta with lower bound 8192", got)
	}
}

func TestProtocolMalformedFramesRejected(t *testing.T) {
	t.Run("BadMagic", func(t *testing.T) {
		var hdr [FrameHeaderSize]byte
		hdr[0], hdr[1] = 'N', 'O'
		hdr[2] = ProtocolMajorVersion
		hdr[3] = byte(MsgStatus)
		if _, err := ReadFrame(bytes.NewReader(hdr[:])); err == nil {
			t.Fatalf("expected error for bad magic")
		}
	})

	t.Run("OversizedPayload", func(t *testing.T) {
		var hdr [FrameHeaderSize]byte
		hdr[0], hdr[1] = FrameMagic0, FrameMagic1
		hdr[2] = ProtocolMajorVersion
		hdr[3] = byte(MsgQueryStart)
		binary.LittleEndian.PutUint32(hdr[4:8], 1)
		binary.LittleEndian.PutUint32(hdr[8:12], MaxPayloadLen+1)
		if _, err := ReadFrame(bytes.NewReader(hdr[:])); err == nil {
			t.Fatalf("expected error for payload > 4 MiB")
		}
	})

	t.Run("TruncatedPayload", func(t *testing.T) {
		var hdr [FrameHeaderSize]byte
		hdr[0], hdr[1] = FrameMagic0, FrameMagic1
		hdr[2] = ProtocolMajorVersion
		hdr[3] = byte(MsgHello)
		binary.LittleEndian.PutUint32(hdr[4:8], 1)
		binary.LittleEndian.PutUint32(hdr[8:12], 50)
		data := append(hdr[:], []byte("short")...)
		if _, err := ReadFrame(bytes.NewReader(data)); err == nil {
			t.Fatalf("expected error for truncated payload")
		}
	})

	t.Run("OverlongUvarint", func(t *testing.T) {
		// 12 continuation bytes -> invalid uvarint overflow
		bad := make([]byte, 14)
		binary.LittleEndian.PutUint16(bad[0:2], 1)
		for i := 2; i < 14; i++ {
			bad[i] = 0x80
		}
		if _, err := DecodeMessage(MsgHello, bad); err == nil {
			t.Fatalf("expected error for overlong uvarint")
		}
	})

	t.Run("CraftedHugeSliceLengthNoOOM", func(t *testing.T) {
		var payload []byte
		payload = binary.LittleEndian.AppendUint64(payload, 1)  // SessionID
		payload = binary.LittleEndian.AppendUint32(payload, 0)  // Offset
		payload = binary.AppendUvarint(payload, math.MaxUint32) // huge row count
		if _, err := DecodeMessage(MsgViewportRows, payload); err == nil {
			t.Fatalf("expected error for crafted huge row count")
		}
	})

	t.Run("UnknownMsgType", func(t *testing.T) {
		if _, err := DecodeMessage(MsgType(250), nil); err == nil {
			t.Fatalf("expected error for unknown msg type")
		}
	})
}

func FuzzDecodeFrame(f *testing.F) {
	// Seed valid frames for each message type plus malformed headers.
	seeds := []any{
		&HelloReq{MajorVersion: 1, ClientName: "cli"},
		&HelloAck{MajorVersion: 1, DaemonPID: 123},
		&QueryStartReq{SessionID: 1, Text: "abc", Sort: 0, Desc: false, PathSep: '/'},
		&QueryResultMeta{SessionID: 1, Total: 10, Retained: 10, Warnings: []string{"w"}},
		&FetchViewportReq{SessionID: 1, Offset: 0, Limit: 60},
		&ViewportRows{SessionID: 1, Offset: 0, Rows: []ViewportRow{{NodeID: 1, Path: "/a", Name: "a", Highlights: []HighlightRange{{Start: 0, End: 1}}}}},
		&CancelReq{SessionID: 1},
		&StatusReq{},
		&StatusReply{ProtocolVersion: 1, LiveCount: 100, ActiveSessions: 1, RejectedConns: 0},
		&ErrorResp{Code: ErrBadRequest, Message: "bad"},
	}
	for _, s := range seeds {
		mt, pl, err := EncodeMessage(s)
		if err != nil {
			continue
		}
		var buf bytes.Buffer
		if err := WriteFrame(&buf, mt, 1, pl); err == nil {
			f.Add(buf.Bytes())
		}
	}
	f.Add([]byte("XF\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00"))
	f.Add([]byte("NO\x01\x01"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		frame, err := ReadFrame(bytes.NewReader(data))
		if err != nil {
			return
		}
		_, _ = DecodeMessage(frame.Type, frame.Payload)
	})
}
