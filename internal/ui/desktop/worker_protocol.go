package desktop

import (
	"encoding/json"
	"io"
)

// WorkerCommand is a JSON-line message sent from the zero-GPU tray supervisor
// to the on-demand MyGo window worker subprocess over stdin.
type WorkerCommand struct {
	Cmd      string   `json:"cmd"`
	Mode     string   `json:"mode,omitempty"`
	Query    string   `json:"query,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// WorkerEvent is a JSON-line message sent from the MyGo window worker subprocess
// back to the tray supervisor over stdout.
type WorkerEvent struct {
	Evt       string `json:"evt"`
	PID       int    `json:"pid,omitempty"`
	Query     string `json:"query,omitempty"`
	Mode      string `json:"mode,omitempty"`
	Created   uint64 `json:"created,omitempty"`
	Destroyed uint64 `json:"destroyed,omitempty"`
	Live      uint64 `json:"live,omitempty"`
}

// WriteJSONLine marshals v as a single newline-terminated JSON frame.
func WriteJSONLine(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = w.Write(b)
	return err
}

func unmarshalJSONLine(line []byte, dst any) error {
	return json.Unmarshal(line, dst)
}
