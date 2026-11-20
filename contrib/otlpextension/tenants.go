package main

import (
	"encoding/json"
	"os"
	"sort"
	"sync"
)

// Tenants is the per-customer on/off switch behind "the customer enables
// CortexDB from the UI". A tenant that was never set follows the default.
// Switches are kept in memory and, when a state file is configured, written
// through to it so a restart does not silently turn a customer off.
type Tenants struct {
	mu       sync.RWMutex
	def      bool
	explicit map[string]bool
	file     string
}

func NewTenants(defaultEnabled bool, stateFile string) (*Tenants, error) {
	t := &Tenants{def: defaultEnabled, explicit: map[string]bool{}, file: stateFile}
	if stateFile == "" {
		return t, nil
	}
	b, err := os.ReadFile(stateFile)
	if os.IsNotExist(err) {
		return t, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &t.explicit); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *Tenants) Enabled(id string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if on, ok := t.explicit[id]; ok {
		return on
	}
	return t.def
}

func (t *Tenants) Set(id string, on bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.explicit[id] = on
	if t.file == "" {
		return nil
	}
	b, err := json.MarshalIndent(t.explicit, "", "  ")
	if err != nil {
		return err
	}
	tmp := t.file + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, t.file)
}

// Snapshot returns the explicitly set tenants and the default for the rest.
func (t *Tenants) Snapshot() (map[string]bool, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]bool, len(t.explicit))
	for k, v := range t.explicit {
		out[k] = v
	}
	return out, t.def
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
