package ipc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

const (
	// FrameMagic0 and FrameMagic1 identify Xift IPC binary frames ("XF").
	FrameMagic0 byte = 'X'
	FrameMagic1 byte = 'F'

	// FrameHeaderSize is the fixed 12-byte binary frame header length:
	// magic(2) | version(1) | type(1) | reqID(4) | payloadLen(4).
	FrameHeaderSize = 12

	// ProtocolMajorVersion is the current wire protocol major version.
	ProtocolMajorVersion = 1

	// MaxPayloadLen is the strict 4 MiB upper bound on any single frame payload.
	MaxPayloadLen = 4 * 1024 * 1024

	// RowFlagFromFoldedScan is stored in ViewportRow.Flags bit 15 to preserve
	// FromFoldedScan across IPC without colliding with storage.HotNode flags.
	RowFlagFromFoldedScan uint16 = 1 << 15
)

// MsgType identifies the payload schema carried by an IPC frame.
type MsgType uint8

const (
	MsgHello              MsgType = 1
	MsgHelloAck           MsgType = 2
	MsgQueryStart         MsgType = 3
	MsgQueryResultMeta    MsgType = 4
	MsgFetchViewport      MsgType = 5
	MsgViewportRows       MsgType = 6
	MsgCancel             MsgType = 7
	MsgStatus             MsgType = 8
	MsgStatusReply        MsgType = 9
	MsgError              MsgType = 10
	MsgRecordOpen         MsgType = 11
	MsgRecordOpenAck      MsgType = 12
	MsgApplyFilterRule    MsgType = 13
	MsgApplyFilterRuleAck MsgType = 14
)

// FilterRuleAction values carried in ApplyFilterRuleReq.Action.
const (
	FilterActionFold        uint8 = 1
	FilterActionHardExclude uint8 = 2
)

// ErrCode classifies protocol and RPC errors returned in ErrorResp.
type ErrCode uint16

const (
	ErrBadRequest      ErrCode = 1
	ErrVersionMismatch ErrCode = 2
	ErrQueryParse      ErrCode = 3
	ErrCanceled        ErrCode = 4
	ErrStaleSession    ErrCode = 5
	ErrBusy            ErrCode = 6
	ErrInternal        ErrCode = 7
)

func (c ErrCode) String() string {
	switch c {
	case ErrBadRequest:
		return "BadRequest"
	case ErrVersionMismatch:
		return "VersionMismatch"
	case ErrQueryParse:
		return "QueryParse"
	case ErrCanceled:
		return "Canceled"
	case ErrStaleSession:
		return "StaleSession"
	case ErrBusy:
		return "Busy"
	case ErrInternal:
		return "Internal"
	default:
		return fmt.Sprintf("ErrCode(%d)", uint16(c))
	}
}

// RPCError represents a structured error returned by the Daemon over IPC.
type RPCError struct {
	Code    ErrCode
	Message string
}

func (e *RPCError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message == "" {
		return "ipc: " + e.Code.String()
	}
	return fmt.Sprintf("ipc: %s: %s", e.Code.String(), e.Message)
}

var (
	ErrInvalidMagic    = errors.New("ipc: invalid frame magic")
	ErrPayloadTooLarge = errors.New("ipc: frame payload exceeds 4 MiB limit")
	ErrMalformedFrame  = errors.New("ipc: malformed frame payload")
)

// FrameHeader represents the decoded 12-byte IPC frame header.
type FrameHeader struct {
	Version    uint8
	Type       MsgType
	ReqID      uint32
	PayloadLen uint32
}

// Frame is a decoded binary frame with header fields and raw payload bytes.
type Frame struct {
	Version uint8
	Type    MsgType
	ReqID   uint32
	Payload []byte
}

// HelloReq initiates a connection handshake.
type HelloReq struct {
	MajorVersion uint16
	ClientName   string
}

// HelloAck acknowledges a successful handshake.
type HelloAck struct {
	MajorVersion uint16
	DaemonPID    uint32
}

// QueryStartReq starts a new query session on the connection.
type QueryStartReq struct {
	SessionID uint64
	Text      string
	Sort      uint8
	Desc      bool
	PathSep   byte
}

// QueryResultMeta is the response to QueryStartReq.
type QueryResultMeta struct {
	SessionID   uint64
	Total       uint64
	Retained    uint32
	CostMicros  uint32
	Incremental bool
	Truncated   bool
	Warnings    []string
}

// FetchViewportReq requests a materialized slice of rows for an active session.
type FetchViewportReq struct {
	SessionID uint64
	Offset    uint32
	Limit     uint16
}

// HighlightRange marks a matched byte range [Start, End) in ViewportRow.Name.
type HighlightRange struct {
	Start uint32
	End   uint32
}

// ViewportRow is a single materialized search result row sent over IPC.
type ViewportRow struct {
	NodeID     uint32
	Flags      uint16
	ExtID      uint16
	VolID      uint16
	Size       int64
	ModTime    int64
	Score      float64
	Path       string
	Name       string
	Highlights []HighlightRange
}

// ViewportRows is the response to FetchViewportReq.
type ViewportRows struct {
	SessionID uint64
	Offset    uint32
	Rows      []ViewportRow
}

// CancelReq cancels and releases the connection's active session.
type CancelReq struct {
	SessionID uint64
}

// StatusReq requests Daemon health and counters.
type StatusReq struct{}

// StatusReply carries Daemon health and counters.
type StatusReply struct {
	ProtocolVersion uint16
	LiveCount       uint64
	ActiveSessions  uint32
	RejectedConns   uint64
}

// ErrorResp carries a structured error response for a request.
type ErrorResp struct {
	Code    ErrCode
	Message string
}

// RecordOpenReq notifies the Daemon that a user opened or revealed a result path.
type RecordOpenReq struct {
	Path string
}

// RecordOpenAck acknowledges a RecordOpenReq.
type RecordOpenAck struct{}

// ApplyFilterRuleReq requests dynamically folding or hard-excluding a directory or path.
type ApplyFilterRuleReq struct {
	Action     uint8
	TargetPath string
	NodeID     uint32
	VolID      uint16
}

// ApplyFilterRuleAck reports how many indexed nodes were affected by ApplyFilterRuleReq.
type ApplyFilterRuleAck struct {
	AffectedCount uint32
}

// DecodeFrameHeader parses a 12-byte frame header and enforces magic and MaxPayloadLen.
func DecodeFrameHeader(hdr []byte) (FrameHeader, error) {
	if len(hdr) < FrameHeaderSize {
		return FrameHeader{}, ErrMalformedFrame
	}
	if hdr[0] != FrameMagic0 || hdr[1] != FrameMagic1 {
		return FrameHeader{}, ErrInvalidMagic
	}
	payloadLen := binary.LittleEndian.Uint32(hdr[8:12])
	if payloadLen > MaxPayloadLen {
		return FrameHeader{}, ErrPayloadTooLarge
	}
	return FrameHeader{
		Version:    hdr[2],
		Type:       MsgType(hdr[3]),
		ReqID:      binary.LittleEndian.Uint32(hdr[4:8]),
		PayloadLen: payloadLen,
	}, nil
}

// WriteFrame writes a complete length-prefixed binary frame to w.
func WriteFrame(w io.Writer, msgType MsgType, reqID uint32, payload []byte) error {
	if len(payload) > MaxPayloadLen {
		return ErrPayloadTooLarge
	}
	var hdr [FrameHeaderSize]byte
	hdr[0] = FrameMagic0
	hdr[1] = FrameMagic1
	hdr[2] = ProtocolMajorVersion
	hdr[3] = byte(msgType)
	binary.LittleEndian.PutUint32(hdr[4:8], reqID)
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(payload)))

	if len(payload) == 0 {
		return writeFull(w, hdr[:])
	}
	buf := make([]byte, FrameHeaderSize+len(payload))
	copy(buf[:FrameHeaderSize], hdr[:])
	copy(buf[FrameHeaderSize:], payload)
	return writeFull(w, buf)
}

func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

// ReadFrame reads and validates a single binary frame from r with MaxPayloadLen (4 MiB).
func ReadFrame(r io.Reader) (Frame, error) {
	return readFrameWithMax(r, MaxPayloadLen)
}

func readFrameWithMax(r io.Reader, maxPayload uint32) (Frame, error) {
	var hdr [FrameHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	h, err := DecodeFrameHeader(hdr[:])
	if err != nil {
		return Frame{}, err
	}
	if h.PayloadLen > maxPayload {
		return Frame{}, ErrPayloadTooLarge
	}
	var payload []byte
	if h.PayloadLen > 0 {
		payload = make([]byte, h.PayloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return Frame{}, err
		}
	}
	return Frame{
		Version: h.Version,
		Type:    h.Type,
		ReqID:   h.ReqID,
		Payload: payload,
	}, nil
}

// EncodeMessage encodes a typed IPC message struct into its MsgType and payload bytes.
func EncodeMessage(msg any) (MsgType, []byte, error) {
	var b []byte
	switch m := msg.(type) {
	case *HelloReq:
		b = make([]byte, 0, 16+len(m.ClientName))
		b = binary.LittleEndian.AppendUint16(b, m.MajorVersion)
		b = appendString(b, m.ClientName)
		return MsgHello, b, nil

	case *HelloAck:
		b = make([]byte, 0, 6)
		b = binary.LittleEndian.AppendUint16(b, m.MajorVersion)
		b = binary.LittleEndian.AppendUint32(b, m.DaemonPID)
		return MsgHelloAck, b, nil

	case *QueryStartReq:
		b = make([]byte, 0, 16+len(m.Text))
		b = binary.LittleEndian.AppendUint64(b, m.SessionID)
		b = appendString(b, m.Text)
		b = append(b, m.Sort, boolToByte(m.Desc), m.PathSep)
		return MsgQueryStart, b, nil

	case *QueryResultMeta:
		b = make([]byte, 0, 32+len(m.Warnings)*16)
		b = binary.LittleEndian.AppendUint64(b, m.SessionID)
		b = binary.LittleEndian.AppendUint64(b, m.Total)
		b = binary.LittleEndian.AppendUint32(b, m.Retained)
		b = binary.LittleEndian.AppendUint32(b, m.CostMicros)
		b = append(b, boolToByte(m.Incremental), boolToByte(m.Truncated))
		b = binary.AppendUvarint(b, uint64(len(m.Warnings)))
		for _, w := range m.Warnings {
			b = appendString(b, w)
		}
		return MsgQueryResultMeta, b, nil

	case *FetchViewportReq:
		b = make([]byte, 0, 14)
		b = binary.LittleEndian.AppendUint64(b, m.SessionID)
		b = binary.LittleEndian.AppendUint32(b, m.Offset)
		b = binary.LittleEndian.AppendUint16(b, m.Limit)
		return MsgFetchViewport, b, nil

	case *ViewportRows:
		b = make([]byte, 0, 16+len(m.Rows)*96)
		b = binary.LittleEndian.AppendUint64(b, m.SessionID)
		b = binary.LittleEndian.AppendUint32(b, m.Offset)
		b = binary.AppendUvarint(b, uint64(len(m.Rows)))
		for i := range m.Rows {
			r := &m.Rows[i]
			b = binary.LittleEndian.AppendUint32(b, r.NodeID)
			b = binary.LittleEndian.AppendUint16(b, r.Flags)
			b = binary.LittleEndian.AppendUint16(b, r.ExtID)
			b = binary.LittleEndian.AppendUint16(b, r.VolID)
			b = binary.LittleEndian.AppendUint64(b, uint64(r.Size))
			b = binary.LittleEndian.AppendUint64(b, uint64(r.ModTime))
			b = binary.LittleEndian.AppendUint64(b, math.Float64bits(r.Score))
			b = appendString(b, r.Path)
			b = appendString(b, r.Name)
			b = binary.AppendUvarint(b, uint64(len(r.Highlights)))
			for _, h := range r.Highlights {
				b = binary.AppendUvarint(b, uint64(h.Start))
				b = binary.AppendUvarint(b, uint64(h.End))
			}
		}
		if len(b) > MaxPayloadLen {
			return 0, nil, ErrPayloadTooLarge
		}
		return MsgViewportRows, b, nil

	case *CancelReq:
		b = make([]byte, 0, 8)
		b = binary.LittleEndian.AppendUint64(b, m.SessionID)
		return MsgCancel, b, nil

	case *StatusReq:
		return MsgStatus, nil, nil

	case *StatusReply:
		b = make([]byte, 0, 22)
		b = binary.LittleEndian.AppendUint16(b, m.ProtocolVersion)
		b = binary.LittleEndian.AppendUint64(b, m.LiveCount)
		b = binary.LittleEndian.AppendUint32(b, m.ActiveSessions)
		b = binary.LittleEndian.AppendUint64(b, m.RejectedConns)
		return MsgStatusReply, b, nil

	case *ErrorResp:
		b = make([]byte, 0, 8+len(m.Message))
		b = binary.LittleEndian.AppendUint16(b, uint16(m.Code))
		b = appendString(b, m.Message)
		return MsgError, b, nil

	case *RecordOpenReq:
		b = make([]byte, 0, 8+len(m.Path))
		b = appendString(b, m.Path)
		return MsgRecordOpen, b, nil

	case *RecordOpenAck:
		return MsgRecordOpenAck, nil, nil

	case *ApplyFilterRuleReq:
		b = make([]byte, 0, 16+len(m.TargetPath))
		b = append(b, m.Action)
		b = binary.LittleEndian.AppendUint32(b, m.NodeID)
		b = binary.LittleEndian.AppendUint16(b, m.VolID)
		b = appendString(b, m.TargetPath)
		return MsgApplyFilterRule, b, nil

	case *ApplyFilterRuleAck:
		b = make([]byte, 0, 4)
		b = binary.LittleEndian.AppendUint32(b, m.AffectedCount)
		return MsgApplyFilterRuleAck, b, nil

	default:
		return 0, nil, fmt.Errorf("ipc: unsupported message type %T", msg)
	}
}

// DecodeMessage decodes a payload buffer into the corresponding typed message struct.
func DecodeMessage(msgType MsgType, payload []byte) (any, error) {
	if len(payload) > MaxPayloadLen {
		return nil, ErrPayloadTooLarge
	}
	d := decoder{b: payload}

	switch msgType {
	case MsgHello:
		ver, err := d.u16()
		if err != nil {
			return nil, err
		}
		name, err := d.str()
		if err != nil {
			return nil, err
		}
		if err := d.done(); err != nil {
			return nil, err
		}
		return &HelloReq{MajorVersion: ver, ClientName: name}, nil

	case MsgHelloAck:
		ver, err := d.u16()
		if err != nil {
			return nil, err
		}
		pid, err := d.u32()
		if err != nil {
			return nil, err
		}
		if err := d.done(); err != nil {
			return nil, err
		}
		return &HelloAck{MajorVersion: ver, DaemonPID: pid}, nil

	case MsgQueryStart:
		sid, err := d.u64()
		if err != nil {
			return nil, err
		}
		text, err := d.str()
		if err != nil {
			return nil, err
		}
		sortVal, err := d.u8()
		if err != nil {
			return nil, err
		}
		desc, err := d.boolVal()
		if err != nil {
			return nil, err
		}
		sep, err := d.u8()
		if err != nil {
			return nil, err
		}
		if err := d.done(); err != nil {
			return nil, err
		}
		return &QueryStartReq{
			SessionID: sid,
			Text:      text,
			Sort:      sortVal,
			Desc:      desc,
			PathSep:   sep,
		}, nil

	case MsgQueryResultMeta:
		sid, err := d.u64()
		if err != nil {
			return nil, err
		}
		total, err := d.u64()
		if err != nil {
			return nil, err
		}
		retained, err := d.u32()
		if err != nil {
			return nil, err
		}
		costMicros, err := d.u32()
		if err != nil {
			return nil, err
		}
		inc, err := d.boolVal()
		if err != nil {
			return nil, err
		}
		trunc, err := d.boolVal()
		if err != nil {
			return nil, err
		}
		wCount, err := d.uvarint()
		if err != nil {
			return nil, err
		}
		// Each warning needs at least 1 byte (length uvarint).
		if wCount > uint64(len(d.b)) {
			return nil, ErrMalformedFrame
		}
		var warnings []string
		if wCount > 0 {
			warnings = make([]string, wCount)
			for i := range warnings {
				w, err := d.str()
				if err != nil {
					return nil, err
				}
				warnings[i] = w
			}
		}
		if err := d.done(); err != nil {
			return nil, err
		}
		return &QueryResultMeta{
			SessionID:   sid,
			Total:       total,
			Retained:    retained,
			CostMicros:  costMicros,
			Incremental: inc,
			Truncated:   trunc,
			Warnings:    warnings,
		}, nil

	case MsgFetchViewport:
		sid, err := d.u64()
		if err != nil {
			return nil, err
		}
		off, err := d.u32()
		if err != nil {
			return nil, err
		}
		lim, err := d.u16()
		if err != nil {
			return nil, err
		}
		if err := d.done(); err != nil {
			return nil, err
		}
		return &FetchViewportReq{SessionID: sid, Offset: off, Limit: lim}, nil

	case MsgViewportRows:
		sid, err := d.u64()
		if err != nil {
			return nil, err
		}
		off, err := d.u32()
		if err != nil {
			return nil, err
		}
		rowCount, err := d.uvarint()
		if err != nil {
			return nil, err
		}
		// Minimum encoded size per ViewportRow is 37 bytes (fixed fields 34B + 3 uvarints).
		if rowCount > uint64(len(d.b)/37) {
			return nil, ErrMalformedFrame
		}
		var rows []ViewportRow
		if rowCount > 0 {
			rows = make([]ViewportRow, rowCount)
			for i := range rows {
				nodeID, err := d.u32()
				if err != nil {
					return nil, err
				}
				flags, err := d.u16()
				if err != nil {
					return nil, err
				}
				extID, err := d.u16()
				if err != nil {
					return nil, err
				}
				volID, err := d.u16()
				if err != nil {
					return nil, err
				}
				sz, err := d.u64()
				if err != nil {
					return nil, err
				}
				mt, err := d.u64()
				if err != nil {
					return nil, err
				}
				scBits, err := d.u64()
				if err != nil {
					return nil, err
				}
				pStr, err := d.str()
				if err != nil {
					return nil, err
				}
				nStr, err := d.str()
				if err != nil {
					return nil, err
				}
				hlCount, err := d.uvarint()
				if err != nil {
					return nil, err
				}
				// Each HighlightRange requires at least 2 bytes (2 uvarints).
				if hlCount > uint64(len(d.b)/2) {
					return nil, ErrMalformedFrame
				}
				var hls []HighlightRange
				if hlCount > 0 {
					hls = make([]HighlightRange, hlCount)
					for j := range hls {
						st, err := d.uvarint()
						if err != nil || st > math.MaxUint32 {
							return nil, ErrMalformedFrame
						}
						en, err := d.uvarint()
						if err != nil || en > math.MaxUint32 {
							return nil, ErrMalformedFrame
						}
						hls[j] = HighlightRange{Start: uint32(st), End: uint32(en)}
					}
				}
				rows[i] = ViewportRow{
					NodeID:     nodeID,
					Flags:      flags,
					ExtID:      extID,
					VolID:      volID,
					Size:       int64(sz),
					ModTime:    int64(mt),
					Score:      math.Float64frombits(scBits),
					Path:       pStr,
					Name:       nStr,
					Highlights: hls,
				}
			}
		}
		if err := d.done(); err != nil {
			return nil, err
		}
		return &ViewportRows{SessionID: sid, Offset: off, Rows: rows}, nil

	case MsgCancel:
		sid, err := d.u64()
		if err != nil {
			return nil, err
		}
		if err := d.done(); err != nil {
			return nil, err
		}
		return &CancelReq{SessionID: sid}, nil

	case MsgStatus:
		if err := d.done(); err != nil {
			return nil, err
		}
		return &StatusReq{}, nil

	case MsgStatusReply:
		ver, err := d.u16()
		if err != nil {
			return nil, err
		}
		live, err := d.u64()
		if err != nil {
			return nil, err
		}
		active, err := d.u32()
		if err != nil {
			return nil, err
		}
		rej, err := d.u64()
		if err != nil {
			return nil, err
		}
		if err := d.done(); err != nil {
			return nil, err
		}
		return &StatusReply{
			ProtocolVersion: ver,
			LiveCount:       live,
			ActiveSessions:  active,
			RejectedConns:   rej,
		}, nil

	case MsgError:
		code, err := d.u16()
		if err != nil {
			return nil, err
		}
		msg, err := d.str()
		if err != nil {
			return nil, err
		}
		if err := d.done(); err != nil {
			return nil, err
		}
		return &ErrorResp{Code: ErrCode(code), Message: msg}, nil

	case MsgRecordOpen:
		p, err := d.str()
		if err != nil {
			return nil, err
		}
		if err := d.done(); err != nil {
			return nil, err
		}
		return &RecordOpenReq{Path: p}, nil

	case MsgRecordOpenAck:
		if err := d.done(); err != nil {
			return nil, err
		}
		return &RecordOpenAck{}, nil

	case MsgApplyFilterRule:
		act, err := d.u8()
		if err != nil {
			return nil, err
		}
		nid, err := d.u32()
		if err != nil {
			return nil, err
		}
		vid, err := d.u16()
		if err != nil {
			return nil, err
		}
		tp, err := d.str()
		if err != nil {
			return nil, err
		}
		if err := d.done(); err != nil {
			return nil, err
		}
		return &ApplyFilterRuleReq{
			Action:     act,
			NodeID:     nid,
			VolID:      vid,
			TargetPath: tp,
		}, nil

	case MsgApplyFilterRuleAck:
		cnt, err := d.u32()
		if err != nil {
			return nil, err
		}
		if err := d.done(); err != nil {
			return nil, err
		}
		return &ApplyFilterRuleAck{AffectedCount: cnt}, nil

	default:
		return nil, fmt.Errorf("%w: unknown message type %d", ErrMalformedFrame, msgType)
	}
}

func boolToByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

func appendString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

type decoder struct {
	b []byte
}

func (d *decoder) u8() (byte, error) {
	if len(d.b) < 1 {
		return 0, ErrMalformedFrame
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v, nil
}

func (d *decoder) boolVal() (bool, error) {
	v, err := d.u8()
	if err != nil {
		return false, err
	}
	if v > 1 {
		return false, ErrMalformedFrame
	}
	return v == 1, nil
}

func (d *decoder) u16() (uint16, error) {
	if len(d.b) < 2 {
		return 0, ErrMalformedFrame
	}
	v := binary.LittleEndian.Uint16(d.b[:2])
	d.b = d.b[2:]
	return v, nil
}

func (d *decoder) u32() (uint32, error) {
	if len(d.b) < 4 {
		return 0, ErrMalformedFrame
	}
	v := binary.LittleEndian.Uint32(d.b[:4])
	d.b = d.b[4:]
	return v, nil
}

func (d *decoder) u64() (uint64, error) {
	if len(d.b) < 8 {
		return 0, ErrMalformedFrame
	}
	v := binary.LittleEndian.Uint64(d.b[:8])
	d.b = d.b[8:]
	return v, nil
}

func (d *decoder) uvarint() (uint64, error) {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		return 0, ErrMalformedFrame
	}
	d.b = d.b[n:]
	return v, nil
}

func (d *decoder) str() (string, error) {
	n, err := d.uvarint()
	if err != nil {
		return "", err
	}
	if n > uint64(len(d.b)) {
		return "", ErrMalformedFrame
	}
	s := string(d.b[:n])
	d.b = d.b[n:]
	return s, nil
}

func (d *decoder) done() error {
	if len(d.b) != 0 {
		return ErrMalformedFrame
	}
	return nil
}
