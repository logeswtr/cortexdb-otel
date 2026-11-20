package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
)

// OTLP/JSON carries trace and span ids as hex strings, unlike every other
// bytes field, which is base64. protojson reads all bytes fields as base64, so
// a hex id would decode into the wrong bytes (a 32-character trace id becomes
// 24 bytes) without any error. fixOTLPJSONIDs rewrites the id fields to base64
// before protojson sees them.
//
// Protobuf senders, which is what the Collector's exporters use by default,
// never go through here.

var idFields = map[string]int{ // field name -> id length in hex characters
	"traceId": 32, "trace_id": 32,
	"spanId": 16, "span_id": 16,
	"parentSpanId": 16, "parent_span_id": 16,
}

func fixOTLPJSONIDs(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // keep 64-bit integers exact
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	walkIDs(doc)
	return json.Marshal(doc)
}

func walkIDs(v any) {
	switch x := v.(type) {
	case map[string]any:
		for k, child := range x {
			if want, ok := idFields[k]; ok {
				if s, ok := child.(string); ok && len(s) == want {
					if b, err := hex.DecodeString(s); err == nil {
						x[k] = base64.StdEncoding.EncodeToString(b)
					}
				}
				continue
			}
			walkIDs(child)
		}
	case []any:
		for _, child := range x {
			walkIDs(child)
		}
	}
}
