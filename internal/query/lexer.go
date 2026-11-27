package query

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type tokenKind uint8

const (
	tokWord tokenKind = iota + 1
	tokOr
	tokLParen
	tokRParen
	tokNot
)

type token struct {
	kind   tokenKind
	text   string
	quoted bool // true when token was quoted (suppresses modifier prefix parsing unless key:"val")
}

// Parse parses a Xift DSL query using time.Now() as the reference clock.
func Parse(input string) (*Query, error) {
	return ParseAt(input, time.Now())
}

// ParseAt parses a Xift DSL query using now as the reference clock for relative dm: filters.
func ParseAt(input string, now time.Time) (*Query, error) {
	toks, err := tokenize(input)
	if err != nil {
		return nil, err
	}
	q := &Query{Raw: input}
	if len(toks) == 0 {
		return q, nil
	}

	p := &parser{
		toks: toks,
		now:  now,
		q:    q,
	}
	expr, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.pos < len(p.toks) {
		return nil, fmt.Errorf("query: unexpected token %q", p.toks[p.pos].text)
	}
	q.Root = expr
	return q, nil
}

func tokenize(input string) ([]token, error) {
	var toks []token
	i := 0
	n := len(input)

	for i < n {
		// Skip whitespace.
		for i < n && isSpace(input[i]) {
			i++
		}
		if i >= n {
			break
		}

		ch := input[i]
		switch ch {
		case '|':
			toks = append(toks, token{kind: tokOr, text: "|"})
			i++
			continue
		case '(':
			toks = append(toks, token{kind: tokLParen, text: "("})
			i++
			continue
		case ')':
			toks = append(toks, token{kind: tokRParen, text: ")"})
			i++
			continue
		case '!':
			toks = append(toks, token{kind: tokNot, text: "!"})
			i++
			continue
		}

		if ch == '"' || ch == '\'' {
			val, next, err := scanQuoted(input, i)
			if err != nil {
				return nil, err
			}
			toks = append(toks, token{kind: tokWord, text: val, quoted: true})
			i = next
			continue
		}

		// Scan an unquoted word; supports key:"quoted value" seamlessly.
		var sb strings.Builder
		for i < n && !isSpace(input[i]) && input[i] != '|' && input[i] != '(' && input[i] != ')' {
			if input[i] == '"' || input[i] == '\'' {
				val, next, err := scanQuoted(input, i)
				if err != nil {
					return nil, err
				}
				sb.WriteString(val)
				i = next
				continue
			}
			sb.WriteByte(input[i])
			i++
		}
		toks = append(toks, token{kind: tokWord, text: sb.String()})
	}
	return toks, nil
}

func scanQuoted(input string, start int) (string, int, error) {
	quote := input[start]
	i := start + 1
	var sb strings.Builder
	for i < len(input) {
		c := input[i]
		if c == '\\' && i+1 < len(input) && (input[i+1] == quote || input[i+1] == '\\') {
			sb.WriteByte(input[i+1])
			i += 2
			continue
		}
		if c == quote {
			return sb.String(), i + 1, nil
		}
		sb.WriteByte(c)
		i++
	}
	return "", i, fmt.Errorf("query: unterminated quoted string")
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}

type parser struct {
	toks []token
	pos  int
	now  time.Time
	q    *Query
}

func (p *parser) parseOr() (*Expr, error) {
	first, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	var branches []*Expr
	if first != nil {
		branches = append(branches, first)
	}
	for p.pos < len(p.toks) && p.toks[p.pos].kind == tokOr {
		p.pos++
		next, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		if next != nil {
			branches = append(branches, next)
		}
	}
	switch len(branches) {
	case 0:
		return nil, nil
	case 1:
		return branches[0], nil
	default:
		return &Expr{Kind: ExprOr, Children: branches}, nil
	}
}

func (p *parser) parseAnd() (*Expr, error) {
	var items []*Expr
	for p.pos < len(p.toks) {
		tk := p.toks[p.pos]
		if tk.kind == tokOr || tk.kind == tokRParen {
			break
		}
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		if expr != nil {
			items = append(items, expr)
		}
	}
	switch len(items) {
	case 0:
		return nil, nil
	case 1:
		return items[0], nil
	default:
		return &Expr{Kind: ExprAnd, Children: items}, nil
	}
}

func (p *parser) parseUnary() (*Expr, error) {
	if p.pos >= len(p.toks) {
		return nil, nil
	}
	if p.toks[p.pos].kind == tokNot {
		p.pos++
		if p.pos >= len(p.toks) {
			return nil, fmt.Errorf("query: dangling '!' negation")
		}
		child, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		if child == nil {
			return nil, fmt.Errorf("query: '!' cannot negate a global modifier")
		}
		return &Expr{Kind: ExprNot, Child: child}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (*Expr, error) {
	if p.pos >= len(p.toks) {
		return nil, nil
	}
	tk := p.toks[p.pos]
	if tk.kind == tokLParen {
		p.pos++
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.pos >= len(p.toks) || p.toks[p.pos].kind != tokRParen {
			return nil, fmt.Errorf("query: missing closing ')'")
		}
		p.pos++
		return inner, nil
	}
	if tk.kind != tokWord {
		return nil, fmt.Errorf("query: unexpected token %q", tk.text)
	}
	p.pos++
	return p.parseWordToken(tk)
}

func (p *parser) parseWordToken(tk token) (*Expr, error) {
	if !tk.quoted {
		if colon := strings.IndexByte(tk.text, ':'); colon > 0 {
			prefix := strings.ToLower(tk.text[:colon])
			val := tk.text[colon+1:]
			switch prefix {
			case "ext":
				return p.parseExtAtom(val)
			case "size":
				return p.parseSizeAtom(val)
			case "dm":
				return p.parseModTimeAtom(val)
			case "path":
				return p.parsePathAtom(val)
			case "type":
				return p.parseTypeAtom(val)
			case "file":
				return p.parseTypeShortcut(false, val)
			case "folder", "dir":
				return p.parseTypeShortcut(true, val)
			case "all", "sys":
				p.q.IncludeSystem = true
				if val == "" {
					return nil, nil
				}
				return makeNameOrWildcardExpr(val), nil
			case "in":
				norm := normalizePathValue(val)
				if norm == "" {
					return nil, fmt.Errorf("query: in: requires a directory name")
				}
				p.q.InDirs = append(p.q.InDirs, norm)
				return nil, nil
			case "content":
				if val == "" {
					return nil, fmt.Errorf("query: content: requires a search term")
				}
				p.q.ContentTerms = append(p.q.ContentTerms, val)
				p.q.Warnings = append(p.q.Warnings, "content: full-text L2 search is scheduled for Phase 3 (P3-02) and is currently ignored")
				return nil, nil
			}
		}
	}
	if tk.text == "" {
		return nil, nil
	}
	return makeNameOrWildcardExpr(tk.text), nil
}

func makeNameOrWildcardExpr(raw string) *Expr {
	lower := strings.ToLower(raw)
	if strings.ContainsAny(lower, "*?") {
		atom := Atom{
			Kind:  AtomWildcard,
			Value: lower,
			Raw:   raw,
		}
		if pure, ok := detectPureExtWildcard(lower); ok {
			atom.PureExt = pure
		}
		return &Expr{Kind: ExprAtom, Atom: atom}
	}
	return &Expr{
		Kind: ExprAtom,
		Atom: Atom{
			Kind:  AtomName,
			Value: lower,
			Raw:   raw,
		},
	}
}

func detectPureExtWildcard(lower string) (string, bool) {
	if len(lower) < 3 || lower[0] != '*' || lower[1] != '.' {
		return "", false
	}
	ext := lower[2:]
	if strings.ContainsAny(ext, "*?./\\") {
		return "", false
	}
	return ext, true
}

func (p *parser) parseExtAtom(val string) (*Expr, error) {
	if val == "" {
		return nil, fmt.Errorf("query: ext: requires at least one extension")
	}
	parts := strings.Split(val, ",")
	exts := make([]string, 0, len(parts))
	for _, part := range parts {
		e := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(part)), ".")
		if e != "" {
			exts = append(exts, e)
		}
	}
	if len(exts) == 0 {
		return nil, fmt.Errorf("query: ext: requires at least one non-empty extension")
	}
	return &Expr{
		Kind: ExprAtom,
		Atom: Atom{
			Kind:  AtomExt,
			Exts:  exts,
			Value: strings.Join(exts, ","),
			Raw:   val,
		},
	}, nil
}

func (p *parser) parsePathAtom(val string) (*Expr, error) {
	norm := normalizePathValue(val)
	if norm == "" {
		return nil, fmt.Errorf("query: path: requires a non-empty pattern")
	}
	return &Expr{
		Kind: ExprAtom,
		Atom: Atom{
			Kind:        AtomPath,
			Value:       norm,
			Raw:         val,
			HasWildcard: strings.ContainsAny(norm, "*?"),
		},
	}, nil
}

func (p *parser) parseTypeAtom(val string) (*Expr, error) {
	switch strings.ToLower(val) {
	case "file", "f":
		return &Expr{Kind: ExprAtom, Atom: Atom{Kind: AtomType, WantDir: false, Value: "file", Raw: val}}, nil
	case "dir", "folder", "d":
		return &Expr{Kind: ExprAtom, Atom: Atom{Kind: AtomType, WantDir: true, Value: "dir", Raw: val}}, nil
	default:
		return nil, fmt.Errorf("query: unsupported type:%q (expected file or dir)", val)
	}
}

func (p *parser) parseTypeShortcut(wantDir bool, val string) (*Expr, error) {
	tExpr := &Expr{Kind: ExprAtom, Atom: Atom{Kind: AtomType, WantDir: wantDir}}
	if val == "" {
		return tExpr, nil
	}
	return &Expr{Kind: ExprAnd, Children: []*Expr{tExpr, makeNameOrWildcardExpr(val)}}, nil
}

func (p *parser) parseSizeAtom(val string) (*Expr, error) {
	if val == "" {
		return nil, fmt.Errorf("query: size: requires a value")
	}
	var sr SizeRange
	if idx := strings.Index(val, ".."); idx >= 0 {
		left := strings.TrimSpace(val[:idx])
		right := strings.TrimSpace(val[idx+2:])
		if left == "" && right == "" {
			return nil, fmt.Errorf("query: invalid empty size range %q", val)
		}
		if left != "" {
			v, err := parseByteSize(left)
			if err != nil {
				return nil, err
			}
			sr.HasMin = true
			sr.Min = v
		}
		if right != "" {
			v, err := parseByteSize(right)
			if err != nil {
				return nil, err
			}
			sr.HasMax = true
			sr.Max = v
		}
		if sr.HasMin && sr.HasMax && sr.Min > sr.Max {
			return nil, fmt.Errorf("query: size range min > max in %q", val)
		}
	} else {
		op, numStr := splitComparisonOp(val)
		v, err := parseByteSize(numStr)
		if err != nil {
			return nil, err
		}
		switch op {
		case ">":
			sr.HasMin = true
			if v == math.MaxInt64 {
				sr.Min = math.MaxInt64
			} else {
				sr.Min = v + 1
			}
		case ">=":
			sr.HasMin = true
			sr.Min = v
		case "<":
			sr.HasMax = true
			if v == 0 {
				sr.Max = -1
			} else {
				sr.Max = v - 1
			}
		case "<=":
			sr.HasMax = true
			sr.Max = v
		case "", "=":
			sr.HasMin = true
			sr.HasMax = true
			sr.Min = v
			sr.Max = v
		}
	}
	return &Expr{
		Kind: ExprAtom,
		Atom: Atom{
			Kind:  AtomSize,
			Size:  sr,
			Value: strings.ToLower(val),
			Raw:   val,
		},
	}, nil
}

func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("query: empty byte size")
	}
	i := 0
	for i < len(s) && ((s[i] >= '0' && s[i] <= '9') || s[i] == '.') {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("query: invalid byte size %q", s)
	}
	numStr := s[:i]
	unit := strings.ToLower(strings.TrimSpace(s[i:]))

	var mult float64
	switch unit {
	case "", "b":
		mult = 1
	case "k", "kb", "kib":
		mult = 1024
	case "m", "mb", "mib":
		mult = 1024 * 1024
	case "g", "gb", "gib":
		mult = 1024 * 1024 * 1024
	case "t", "tb", "tib":
		mult = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("query: unknown size unit %q in %q", unit, s)
	}

	if !strings.ContainsRune(numStr, '.') {
		iv, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil || iv < 0 {
			return 0, fmt.Errorf("query: invalid size number %q", s)
		}
		imult := int64(mult)
		if iv > math.MaxInt64/imult {
			return 0, fmt.Errorf("query: size overflow %q", s)
		}
		return iv * imult, nil
	}
	fv, err := strconv.ParseFloat(numStr, 64)
	if err != nil || fv < 0 || math.IsNaN(fv) || math.IsInf(fv, 0) {
		return 0, fmt.Errorf("query: invalid size number %q", s)
	}
	res := fv * mult
	if res > float64(math.MaxInt64) {
		return 0, fmt.Errorf("query: size overflow %q", s)
	}
	return int64(res), nil
}

func (p *parser) parseModTimeAtom(val string) (*Expr, error) {
	if val == "" {
		return nil, fmt.Errorf("query: dm: requires a time expression")
	}
	tr, err := parseTimeFilter(val, p.now)
	if err != nil {
		return nil, err
	}
	return &Expr{
		Kind: ExprAtom,
		Atom: Atom{
			Kind:    AtomModTime,
			ModTime: tr,
			Value:   strings.ToLower(val),
			Raw:     val,
		},
	}, nil
}

func parseTimeFilter(val string, now time.Time) (TimeRange, error) {
	var tr TimeRange
	if idx := strings.Index(val, ".."); idx >= 0 {
		left := strings.TrimSpace(val[:idx])
		right := strings.TrimSpace(val[idx+2:])
		if left == "" && right == "" {
			return tr, fmt.Errorf("query: empty dm range")
		}
		if left != "" {
			start, _, err := parseAbsoluteTime(left, now)
			if err != nil {
				return tr, err
			}
			tr.HasMin = true
			tr.MinNano = start.UnixNano()
		}
		if right != "" {
			_, end, err := parseAbsoluteTime(right, now)
			if err != nil {
				return tr, err
			}
			tr.HasMax = true
			tr.MaxNano = end.UnixNano()
		}
		if tr.HasMin && tr.HasMax && tr.MinNano > tr.MaxNano {
			return tr, fmt.Errorf("query: dm range start > end in %q", val)
		}
		return tr, nil
	}

	lower := strings.ToLower(val)
	if lower == "today" {
		y, m, d := now.Date()
		start := time.Date(y, m, d, 0, 0, 0, 0, now.Location())
		tr.HasMin = true
		tr.MinNano = start.UnixNano()
		return tr, nil
	}
	if lower == "yesterday" {
		y, m, d := now.Date()
		todayStart := time.Date(y, m, d, 0, 0, 0, 0, now.Location())
		tr.HasMin = true
		tr.MinNano = todayStart.AddDate(0, 0, -1).UnixNano()
		tr.HasMax = true
		tr.MaxNano = todayStart.UnixNano() - 1
		return tr, nil
	}

	op, rest := splitComparisonOp(val)
	if dur, ok := parseRelativeDuration(rest); ok {
		cutoff := now.Add(-dur).UnixNano()
		switch op {
		case "", "<", "<=":
			// dm:7d or dm:<7d means modified within the last 7 days (>= now - 7d).
			tr.HasMin = true
			tr.MinNano = cutoff
		case ">", ">=":
			// dm:>7d means older than 7 days (<= now - 7d).
			tr.HasMax = true
			tr.MaxNano = cutoff
		case "=":
			tr.HasMin = true
			tr.MinNano = cutoff
		}
		return tr, nil
	}

	start, end, err := parseAbsoluteTime(rest, now)
	if err != nil {
		return tr, err
	}
	switch op {
	case ">":
		tr.HasMin = true
		tr.MinNano = end.UnixNano() + 1
	case ">=":
		tr.HasMin = true
		tr.MinNano = start.UnixNano()
	case "<":
		tr.HasMax = true
		tr.MaxNano = start.UnixNano() - 1
	case "<=":
		tr.HasMax = true
		tr.MaxNano = end.UnixNano()
	case "", "=":
		tr.HasMin = true
		tr.MinNano = start.UnixNano()
		tr.HasMax = true
		tr.MaxNano = end.UnixNano()
	}
	return tr, nil
}

func parseRelativeDuration(s string) (time.Duration, bool) {
	if len(s) < 2 {
		return 0, false
	}
	unit := s[len(s)-1]
	numPart := s[:len(s)-1]
	v, err := strconv.ParseInt(numPart, 10, 64)
	if err != nil || v < 0 {
		return 0, false
	}
	switch unit {
	case 's', 'S':
		return time.Duration(v) * time.Second, true
	case 'm':
		return time.Duration(v) * time.Minute, true
	case 'h', 'H':
		return time.Duration(v) * time.Hour, true
	case 'd', 'D':
		return time.Duration(v) * 24 * time.Hour, true
	case 'w', 'W':
		return time.Duration(v) * 7 * 24 * time.Hour, true
	case 'M':
		return time.Duration(v) * 30 * 24 * time.Hour, true
	case 'y', 'Y':
		return time.Duration(v) * 365 * 24 * time.Hour, true
	default:
		return 0, false
	}
}

func parseAbsoluteTime(s string, now time.Time) (start time.Time, end time.Time, err error) {
	if t, e := time.Parse(time.RFC3339, s); e == nil {
		return t, t, nil
	}
	if t, e := time.ParseInLocation("2006-01-02", s, now.Location()); e == nil {
		return t, t.AddDate(0, 0, 1).Add(-time.Nanosecond), nil
	}
	if t, e := time.ParseInLocation("2006-01", s, now.Location()); e == nil {
		return t, t.AddDate(0, 1, 0).Add(-time.Nanosecond), nil
	}
	if len(s) == 4 {
		if y, e := strconv.Atoi(s); e == nil && y >= 1970 && y <= 9999 {
			t := time.Date(y, 1, 1, 0, 0, 0, 0, now.Location())
			return t, t.AddDate(1, 0, 0).Add(-time.Nanosecond), nil
		}
	}
	return time.Time{}, time.Time{}, fmt.Errorf("query: invalid dm: time %q", s)
}

func splitComparisonOp(s string) (op string, rest string) {
	if strings.HasPrefix(s, ">=") || strings.HasPrefix(s, "<=") {
		return s[:2], s[2:]
	}
	if strings.HasPrefix(s, ">") || strings.HasPrefix(s, "<") || strings.HasPrefix(s, "=") {
		return s[:1], s[1:]
	}
	return "", s
}

func normalizePathValue(s string) string {
	s = strings.ReplaceAll(strings.TrimSpace(s), "\\", "/")
	s = strings.Trim(s, "/")
	return strings.ToLower(s)
}
