package desktop

import (
	"unicode/utf8"

	"github.com/egoist/mygo/ui"
	"github.com/mr-dadong/Xift/internal/ipc"
)

// BuildHighlightSpans slices name according to backend-supplied UTF-8 byte HighlightRanges
// into ui.Span runs without performing any string matching in the UI thread.
// Out-of-bounds, unsorted, overlapping, or mid-rune offsets are defensively clamped.
func BuildHighlightSpans(name string, ranges []ipc.HighlightRange, baseColor, hlColor, hlBg ui.Color) []ui.Span {
	if name == "" {
		return nil
	}
	if len(ranges) == 0 {
		return []ui.Span{{Text: name, Color: baseColor}}
	}

	n := len(name)
	spans := make([]ui.Span, 0, len(ranges)*2+1)
	cursor := 0

	for _, r := range ranges {
		s := int(r.Start)
		e := int(r.End)
		if s < cursor {
			s = cursor
		}
		if e > n {
			e = n
		}
		// Snap defensively to valid UTF-8 rune boundaries.
		for s < n && !utf8.RuneStart(name[s]) {
			s++
		}
		for e > s && e < n && !utf8.RuneStart(name[e]) {
			e--
		}
		if e <= s {
			continue
		}
		if s > cursor {
			spans = append(spans, ui.Span{
				Text:  name[cursor:s],
				Color: baseColor,
			})
		}
		spans = append(spans, ui.Span{
			Text:       name[s:e],
			Color:      hlColor,
			Background: hlBg,
			Weight:     600,
			Radius:     3,
		})
		cursor = e
	}
	if cursor < n {
		spans = append(spans, ui.Span{
			Text:  name[cursor:],
			Color: baseColor,
		})
	}
	if len(spans) == 0 {
		return []ui.Span{{Text: name, Color: baseColor}}
	}
	return spans
}
