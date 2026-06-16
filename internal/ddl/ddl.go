package ddl

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/partforge/partforge/internal/chhttp"
)

func NormalizeCreateTable(query string) (string, error) {
	q := strings.TrimSpace(query)
	if q == "" {
		return "", fmt.Errorf("empty CREATE TABLE")
	}
	q = strings.TrimSuffix(q, ";")

	engineStart, engineEnd, engine, args, hasArgs, err := parseEngine(q)
	if err != nil {
		return "", err
	}

	normalizedEngine, err := normalizeEngine(engine, args, hasArgs)
	if err != nil {
		return "", err
	}
	return q[:engineStart] + normalizedEngine + q[engineEnd:], nil
}

func ForTable(query, database, table string) (string, error) {
	q, err := NormalizeCreateTable(query)
	if err != nil {
		return "", err
	}
	start, end, err := tableNameSpan(q)
	if err != nil {
		return "", err
	}
	return q[:start] + chhttp.TableSQL(database, table) + q[end:], nil
}

func normalizeEngine(engine string, args string, hasArgs bool) (string, error) {
	if strings.HasPrefix(engine, "Replicated") && strings.HasSuffix(engine, "MergeTree") {
		baseEngine := strings.TrimPrefix(engine, "Replicated")
		if baseEngine == "" {
			baseEngine = "MergeTree"
		}
		if !hasArgs {
			return "", fmt.Errorf("%s requires ZooKeeper path and replica arguments", engine)
		}
		parts, err := splitTopLevelArgs(args)
		if err != nil {
			return "", err
		}
		if len(parts) < 2 {
			return "", fmt.Errorf("%s requires at least two replication arguments", engine)
		}
		remaining := parts[2:]
		if len(remaining) == 0 {
			return baseEngine, nil
		}
		return baseEngine + "(" + strings.Join(remaining, ", ") + ")", nil
	}

	if !strings.HasSuffix(engine, "MergeTree") {
		return "", fmt.Errorf("unsupported engine %s; only MergeTree family engines are supported", engine)
	}
	if hasArgs {
		return engine + "(" + args + ")", nil
	}
	return engine, nil
}

func parseEngine(q string) (start int, end int, engine string, args string, hasArgs bool, err error) {
	idx := indexKeyword(q, "ENGINE")
	if idx < 0 {
		err = fmt.Errorf("CREATE TABLE does not contain ENGINE")
		return
	}
	pos := idx + len("ENGINE")
	pos = skipSpaces(q, pos)
	if pos >= len(q) || q[pos] != '=' {
		err = fmt.Errorf("ENGINE clause is missing '='")
		return
	}
	pos++
	pos = skipSpaces(q, pos)
	start = pos

	nameStart := pos
	for pos < len(q) && (unicode.IsLetter(rune(q[pos])) || unicode.IsDigit(rune(q[pos])) || q[pos] == '_') {
		pos++
	}
	if nameStart == pos {
		err = fmt.Errorf("ENGINE clause is missing engine name")
		return
	}
	engine = q[nameStart:pos]
	nameEnd := pos
	pos = skipSpaces(q, pos)
	if pos < len(q) && q[pos] == '(' {
		var close int
		args, close, err = readParens(q, pos)
		if err != nil {
			return
		}
		end = close + 1
		hasArgs = true
		return
	}
	end = nameEnd
	return
}

func tableNameSpan(q string) (int, int, error) {
	pos := skipSpaces(q, 0)
	if !consumeKeyword(q, &pos, "CREATE") || !consumeKeyword(q, &pos, "TABLE") {
		return 0, 0, fmt.Errorf("query is not CREATE TABLE")
	}
	_ = consumeKeyword(q, &pos, "IF")
	if strings.EqualFold(nextWord(q, pos), "NOT") {
		_ = consumeKeyword(q, &pos, "NOT")
		if !consumeKeyword(q, &pos, "EXISTS") {
			return 0, 0, fmt.Errorf("malformed IF NOT EXISTS clause")
		}
	}
	start := skipSpaces(q, pos)
	pos = start
	if err := consumeIdentifier(q, &pos); err != nil {
		return 0, 0, err
	}
	pos = skipSpaces(q, pos)
	if pos < len(q) && q[pos] == '.' {
		pos++
		pos = skipSpaces(q, pos)
		if err := consumeIdentifier(q, &pos); err != nil {
			return 0, 0, err
		}
	}
	return start, pos, nil
}

func consumeIdentifier(q string, pos *int) error {
	if *pos >= len(q) {
		return fmt.Errorf("expected identifier")
	}
	if q[*pos] == '`' {
		*pos += 1
		for *pos < len(q) {
			if q[*pos] == '`' {
				if *pos+1 < len(q) && q[*pos+1] == '`' {
					*pos += 2
					continue
				}
				*pos += 1
				return nil
			}
			*pos += 1
		}
		return fmt.Errorf("unterminated quoted identifier")
	}
	start := *pos
	for *pos < len(q) {
		ch := q[*pos]
		if unicode.IsSpace(rune(ch)) || ch == '.' || ch == '(' {
			break
		}
		*pos++
	}
	if *pos == start {
		return fmt.Errorf("expected identifier")
	}
	return nil
}

func splitTopLevelArgs(s string) ([]string, error) {
	var args []string
	start := 0
	depth := 0
	var quote byte
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if quote != 0 {
			if ch == '\\' {
				i++
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return nil, fmt.Errorf("unbalanced ')' in engine args")
			}
			depth--
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote in engine args")
	}
	if depth != 0 {
		return nil, fmt.Errorf("unbalanced '(' in engine args")
	}
	args = append(args, strings.TrimSpace(s[start:]))
	return args, nil
}

func readParens(q string, open int) (string, int, error) {
	depth := 0
	var quote byte
	for i := open; i < len(q); i++ {
		ch := q[i]
		if quote != 0 {
			if ch == '\\' {
				i++
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return q[open+1 : i], i, nil
			}
		}
	}
	return "", 0, fmt.Errorf("unterminated engine arguments")
}

func indexKeyword(s, keyword string) int {
	upper := strings.ToUpper(s)
	needle := strings.ToUpper(keyword)
	for offset := 0; ; {
		idx := strings.Index(upper[offset:], needle)
		if idx < 0 {
			return -1
		}
		idx += offset
		beforeOK := idx == 0 || !isIdent(s[idx-1])
		after := idx + len(keyword)
		afterOK := after == len(s) || !isIdent(s[after])
		if beforeOK && afterOK {
			return idx
		}
		offset = idx + len(keyword)
	}
}

func consumeKeyword(q string, pos *int, keyword string) bool {
	*pos = skipSpaces(q, *pos)
	word := nextWord(q, *pos)
	if !strings.EqualFold(word, keyword) {
		return false
	}
	*pos += len(word)
	return true
}

func nextWord(q string, pos int) string {
	pos = skipSpaces(q, pos)
	start := pos
	for pos < len(q) && isIdent(q[pos]) {
		pos++
	}
	return q[start:pos]
}

func skipSpaces(s string, pos int) int {
	for pos < len(s) && unicode.IsSpace(rune(s[pos])) {
		pos++
	}
	return pos
}

func isIdent(ch byte) bool {
	return ch == '_' || unicode.IsLetter(rune(ch)) || unicode.IsDigit(rune(ch))
}
