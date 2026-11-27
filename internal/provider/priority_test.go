package provider

import "testing"

func TestEventTypeString(t *testing.T) {
	cases := map[EventType]string{
		EventCreate:     "create",
		EventDelete:     "delete",
		EventModifyMeta: "modify_meta",
		EventRename:     "rename",
		EventRescanDir:  "rescan_dir",
		EventType(99):   "unknown_event(99)",
	}
	for ev, want := range cases {
		if got := ev.String(); got != want {
			t.Fatalf("EventType(%d).String() = %q, want %q", ev, got, want)
		}
	}
}

func TestSetLowPriorityThreadIdempotentRestore(t *testing.T) {
	restore := SetLowPriorityThread()
	if restore == nil {
		t.Fatal("SetLowPriorityThread returned nil restore func")
	}
	restore()
	// Calling restore twice must be safe and idempotent.
	restore()
}
