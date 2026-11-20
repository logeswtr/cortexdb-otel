package main

import (
	"crypto/sha1"
	"encoding/hex"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// Messages that differ only in an id, a number or a quoted value are the same
// incident. Template reduces a message to its shape so they share one
// fingerprint instead of each becoming a memory of its own.
var (
	reUUID   = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	reIP     = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b`)
	reHex    = regexp.MustCompile(`(?i)\b0x[0-9a-f]+\b|\b[0-9a-f]{16,}\b`)
	reQuoted = regexp.MustCompile(`"[^"]*"|'[^']*'`)
	// No trailing \b: durations and sizes run straight into their unit
	// ("5012ms", "64MB"), and there is no word boundary between digit and
	// letter. The leading \b keeps identifiers such as "h2" or "v3" intact.
	reNum   = regexp.MustCompile(`\b\d+(?:\.\d+)?`)
	reSpace = regexp.MustCompile(`\s+`)
)

const maxTemplateRunes = 300

// Template returns s with ids, addresses, numbers and quoted values replaced
// by placeholders, whitespace collapsed and the length bounded.
func Template(s string) string {
	s = truncate(strings.TrimSpace(s), 1000)
	s = reUUID.ReplaceAllString(s, "<uuid>")
	s = reIP.ReplaceAllString(s, "<ip>")
	s = reHex.ReplaceAllString(s, "<hex>")
	s = reQuoted.ReplaceAllString(s, "<str>")
	s = reNum.ReplaceAllString(s, "<n>")
	s = reSpace.ReplaceAllString(s, " ")
	return truncate(s, maxTemplateRunes)
}

// Fingerprint names an incident: the same parts always give the same id, so
// the same incident rewrites one memory instead of adding a new one.
func Fingerprint(parts ...string) string {
	h := sha1.Sum([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(h[:8])
}

func truncate(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	r := []rune(s)
	return string(r[:max]) + "…"
}

// sanitizeTenant keeps a tenant id usable as a CortexDB namespace and as part
// of an entity id. Anything outside [A-Za-z0-9._-] becomes an underscore.
func sanitizeTenant(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 64 {
		s = s[:64]
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			return r
		}
		return '_'
	}, s)
}

// attrStr reads an attribute as text; numbers and booleans are formatted.
func attrStr(kvs []*commonpb.KeyValue, key string) string {
	for _, kv := range kvs {
		if kv.GetKey() != key {
			continue
		}
		v := kv.GetValue()
		switch x := v.GetValue().(type) {
		case *commonpb.AnyValue_StringValue:
			return x.StringValue
		case *commonpb.AnyValue_IntValue:
			return strconv.FormatInt(x.IntValue, 10)
		case *commonpb.AnyValue_BoolValue:
			if x.BoolValue {
				return "true"
			}
			return "false"
		case *commonpb.AnyValue_DoubleValue:
			return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
		}
		return ""
	}
	return ""
}

// entityKey is the id a service is stored under: tenant and service joined by
// a double underscore after each is reduced to [a-z0-9-]. CortexDB derives
// node ids from names by dropping every other character, so building the key
// here keeps "a/bc" and "ab/c" from colliding.
func entityKey(tenant, name string) string {
	return keyPart(tenant) + "__" + keyPart(name)
}

func keyPart(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "unknown"
	}
	return out
}
