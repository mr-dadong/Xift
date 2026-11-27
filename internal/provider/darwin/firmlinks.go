package darwin

import (
	"os"
	"strings"
	"sync"
)

const (
	// DefaultFirmlinksPath is the system APFS firmlink mapping table on macOS.
	DefaultFirmlinksPath = "/usr/share/firmlinks"
	dataVolumePrefix     = "/System/Volumes/Data/"
	maxDarwinPathCache   = 200_000
)

// Firmlink describes a single macOS APFS firmlink entry from /usr/share/firmlinks
// mapping a canonical root path (System) to its backing path under /System/Volumes/Data (Data).
type Firmlink struct {
	SystemPath string // e.g., "/Users"
	DataRel    string // e.g., "Users"
	DataPath   string // e.g., "/System/Volumes/Data/Users"
}

// ParseFirmlinks parses the contents of /usr/share/firmlinks.
// Prefers tab-delimited splitting to preserve spaces inside paths (e.g., "/Library/Application Support").
func ParseFirmlinks(content string) []Firmlink {
	var links []Firmlink
	for _, rawLine := range strings.Split(content, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var sysRaw, dataRaw string
		if tabIdx := strings.IndexByte(line, '\t'); tabIdx >= 0 {
			sysRaw = strings.TrimSpace(line[:tabIdx])
			dataRaw = strings.TrimSpace(line[tabIdx+1:])
		} else {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			sysRaw = fields[0]
			dataRaw = fields[1]
		}
		sysPath := normalizeUnixPath(sysRaw)
		dataRel := strings.TrimPrefix(normalizeUnixPath(dataRaw), "/")
		if sysPath == "" || dataRel == "" {
			continue
		}
		links = append(links, Firmlink{
			SystemPath: sysPath,
			DataRel:    dataRel,
			DataPath:   dataVolumePrefix + dataRel,
		})
	}
	return links
}

// LoadDefaultFirmlinks reads /usr/share/firmlinks strictly read-only, returning nil if absent.
func LoadDefaultFirmlinks() []Firmlink {
	data, err := os.ReadFile(DefaultFirmlinksPath)
	if err != nil {
		return nil
	}
	return ParseFirmlinks(string(data))
}

// FirmlinkDeduper prevents double-indexing of APFS firmlinked directories when scanning "/"
// and canonicalizes FSEvents paths arriving under /System/Volumes/Data/.
type FirmlinkDeduper struct {
	links []Firmlink
}

// NewFirmlinkDeduper builds a FirmlinkDeduper from parsed firmlink entries.
func NewFirmlinkDeduper(links []Firmlink) *FirmlinkDeduper {
	cp := make([]Firmlink, len(links))
	copy(cp, links)
	return &FirmlinkDeduper{links: cp}
}

// IsDuplicateDataPath reports whether p lies under /System/Volumes/Data/<firmlink-target>
// and is therefore already reachable via its canonical firmlink SystemPath under "/".
func (d *FirmlinkDeduper) IsDuplicateDataPath(p string) bool {
	if d == nil || len(d.links) == 0 {
		return false
	}
	norm := normalizeUnixPath(p)
	if !strings.HasPrefix(norm, "/System/Volumes/Data/") {
		return false
	}
	for i := range d.links {
		dp := d.links[i].DataPath
		if norm == dp || (len(norm) > len(dp) && strings.HasPrefix(norm, dp) && norm[len(dp)] == '/') {
			return true
		}
	}
	return false
}

// Canonicalize rewrites any /System/Volumes/Data/<firmlink-target>/... path back to its
// canonical firmlink path (e.g., /System/Volumes/Data/Users/alice -> /Users/alice).
func (d *FirmlinkDeduper) Canonicalize(p string) string {
	norm := normalizeUnixPath(p)
	if d == nil || len(d.links) == 0 {
		return norm
	}
	if !strings.HasPrefix(norm, "/System/Volumes/Data/") {
		return norm
	}
	for i := range d.links {
		dp := d.links[i].DataPath
		if norm == dp {
			return d.links[i].SystemPath
		}
		if len(norm) > len(dp) && strings.HasPrefix(norm, dp) && norm[len(dp)] == '/' {
			return d.links[i].SystemPath + norm[len(dp):]
		}
	}
	return norm
}

// IsTimeMachineSnapshotPath reports whether p is a macOS Time Machine local snapshot
// or mobile backup mount that MUST be excluded from indexing.
func IsTimeMachineSnapshotPath(p string) bool {
	norm := normalizeUnixPath(p)
	if norm == "/.MobileBackups" || strings.HasPrefix(norm, "/.MobileBackups/") {
		return true
	}
	if norm == "/Volumes/.timemachine" || strings.HasPrefix(norm, "/Volumes/.timemachine/") {
		return true
	}
	if strings.HasPrefix(norm, "/Volumes/com.apple.TimeMachine.") {
		return true
	}
	return false
}

// IsAuxiliaryMacOSMount reports pseudo/auxiliary mounts skipped when scanning "/".
func IsAuxiliaryMacOSMount(p string) bool {
	norm := normalizeUnixPath(p)
	switch norm {
	case "/dev", "/Volumes",
		"/System/Volumes/VM",
		"/System/Volumes/Preboot",
		"/System/Volumes/Update",
		"/System/Volumes/xarts",
		"/System/Volumes/iSCPreboot",
		"/System/Volumes/Hardware":
		return true
	}
	return strings.HasPrefix(norm, "/dev/") || strings.HasPrefix(norm, "/Volumes/")
}

func normalizeUnixPath(p string) string {
	if p == "" {
		return ""
	}
	p = strings.ReplaceAll(p, `\`, "/")
	for len(p) > 1 && strings.HasSuffix(p, "/") {
		p = p[:len(p)-1]
	}
	return p
}

// CachedNode stores real APFS inode metadata for path-to-inode resolution across FSEvents deletes/renames.
type CachedNode struct {
	NodeKey   uint64
	ParentKey uint64
	IsDir     bool
}

// PathKeyCache is a bounded thread-safe canonical path -> APFS inode cache.
type PathKeyCache struct {
	mu      sync.RWMutex
	entries map[string]CachedNode
}

// NewPathKeyCache creates a new PathKeyCache.
func NewPathKeyCache() *PathKeyCache {
	return &PathKeyCache{
		entries: make(map[string]CachedNode, 1024),
	}
}

// Put stores or updates a canonical path's APFS inode mapping.
func (c *PathKeyCache) Put(fullPath string, ent CachedNode) {
	if c == nil || fullPath == "" || ent.NodeKey == 0 {
		return
	}
	key := normalizeUnixPath(fullPath)
	c.mu.Lock()
	if len(c.entries) >= maxDarwinPathCache {
		if _, exists := c.entries[key]; !exists {
			c.mu.Unlock()
			return
		}
	}
	c.entries[key] = ent
	c.mu.Unlock()
}

// Get looks up a cached APFS inode for fullPath.
func (c *PathKeyCache) Get(fullPath string) (CachedNode, bool) {
	if c == nil || fullPath == "" {
		return CachedNode{}, false
	}
	key := normalizeUnixPath(fullPath)
	c.mu.RLock()
	ent, ok := c.entries[key]
	c.mu.RUnlock()
	return ent, ok
}

// Delete removes fullPath (and cached descendants if it is a directory) and returns its prior entry.
func (c *PathKeyCache) Delete(fullPath string) (CachedNode, bool) {
	if c == nil || fullPath == "" {
		return CachedNode{}, false
	}
	key := normalizeUnixPath(fullPath)
	c.mu.Lock()
	ent, ok := c.entries[key]
	if ok {
		delete(c.entries, key)
		if ent.IsDir && key != "/" {
			prefix := key + "/"
			for k := range c.entries {
				if strings.HasPrefix(k, prefix) {
					delete(c.entries, k)
				}
			}
		}
	}
	c.mu.Unlock()
	return ent, ok
}

// RenamePrefix re-keys cached descendants when a directory moves from oldPath to newPath.
func (c *PathKeyCache) RenamePrefix(oldPath, newPath string) {
	if c == nil || oldPath == "" || newPath == "" {
		return
	}
	oldKey := normalizeUnixPath(oldPath)
	newKey := normalizeUnixPath(newPath)
	if oldKey == newKey || oldKey == "/" {
		return
	}
	oldPrefix := oldKey + "/"
	newPrefix := newKey + "/"

	c.mu.Lock()
	var updates []struct {
		k   string
		ent CachedNode
	}
	for k, ent := range c.entries {
		if strings.HasPrefix(k, oldPrefix) {
			updates = append(updates, struct {
				k   string
				ent CachedNode
			}{k: newPrefix + k[len(oldPrefix):], ent: ent})
			delete(c.entries, k)
		}
	}
	for _, u := range updates {
		c.entries[u.k] = u.ent
	}
	c.mu.Unlock()
}
