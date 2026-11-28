package lsm

import (
	"errors"
	"sync"
)

// DefaultBlockCacheBytes is the default hard byte budget of the Tier 2 block
// cache (architecture rule 4: fixed-budget LRU block cache, default 128 MB).
const DefaultBlockCacheBytes = 128 << 20

// blockCacheShards is the sharding factor; each shard owns limit/shards bytes,
// so the sum over shards never exceeds the total budget.
const blockCacheShards = 64

// BlockKind identifies which Tier 2 segment file a cached block belongs to.
type BlockKind uint8

const (
	BlockKindNames    BlockKind = iota + 1 // decoded front-coded name block
	BlockKindInverted                      // serialized posting-list bytes
	BlockKindNameIdx                       // names.idx entry chunk
	BlockKindDelBits                       // tombstone bitmap chunk
	BlockKindHot                           // hot.col record block (Tier 2)
	BlockKindKeys                          // keys.idx record block (Tier 2)
	BlockKindNodeIDs                       // nodeids.col record block (Tier 2)
)

var (
	// ErrBlockCacheValue is returned when a value is not a flat []byte block.
	ErrBlockCacheValue = errors.New("lsm: block cache value must be []byte")
	// ErrBlockCacheOversized is returned when a value cannot fit within one
	// shard budget (total/64); oversized blocks bypass the cache.
	ErrBlockCacheOversized = errors.New("lsm: block cache value exceeds shard budget")
)

// cacheKey identifies one cached block across (segment, file kind, block).
type cacheKey struct {
	segID   uint32
	kind    BlockKind
	_       [3]uint8
	blockID uint64
}

// cacheEntry is an intrusive doubly-linked-list node; MRU sits next to the
// sentinel head, LRU next to the sentinel tail.
type cacheEntry struct {
	key  cacheKey
	val  []byte
	prev *cacheEntry
	next *cacheEntry
}

// cacheShard is one mutex-protected shard of the LRU with byte accounting.
type cacheShard struct {
	mu      sync.Mutex
	entries map[cacheKey]*cacheEntry
	head    cacheEntry // sentinel: head.next = MRU, head.prev = LRU
	bytes   int64
	limit   int64
}

func (s *cacheShard) init(limit int64) {
	s.entries = make(map[cacheKey]*cacheEntry, 64)
	s.head.next = &s.head
	s.head.prev = &s.head
	s.bytes = 0
	s.limit = limit
}

// BlockCache is a fixed-budget sharded LRU cache over flat []byte blocks.
// Get returns the shared cached slice: callers MUST NOT mutate it.
type BlockCache struct {
	shards [blockCacheShards]cacheShard
	limit  int64
}

// NewBlockCache builds a cache with the given total byte budget. A zero or
// negative budget selects DefaultBlockCacheBytes. Every shard receives
// limit/64 bytes, so the cache total can never exceed limit.
func NewBlockCache(limit int64) *BlockCache {
	if limit <= 0 {
		limit = DefaultBlockCacheBytes
	}
	c := &BlockCache{limit: limit}
	shardLimit := limit / blockCacheShards
	if shardLimit < 1 {
		shardLimit = 1
	}
	for i := range c.shards {
		c.shards[i].init(shardLimit)
	}
	return c
}

// Limit returns the configured total byte budget.
func (c *BlockCache) Limit() int64 { return c.limit }

// Bytes returns the current total resident bytes (approximate under
// concurrency; exact once writers quiesce).
func (c *BlockCache) Bytes() int64 {
	var total int64
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		total += s.bytes
		s.mu.Unlock()
	}
	return total
}

// Close drops all cached blocks.
func (c *BlockCache) Close() {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		s.init(s.limit)
		s.mu.Unlock()
	}
}

func (k cacheKey) hash() uint64 {
	// FNV-1a over the key fields.
	h := uint64(14695981039346656037)
	mix := func(v uint64) {
		for i := 0; i < 8; i++ {
			h ^= v & 0xff
			h *= 1099511628211
			v >>= 8
		}
	}
	mix(uint64(k.segID) | uint64(k.kind)<<32)
	mix(k.blockID)
	return h
}

func (c *BlockCache) shardFor(key cacheKey) *cacheShard {
	return &c.shards[key.hash()%blockCacheShards]
}

// Get returns the cached block for (segID, kind, blockID), refreshing its LRU
// position. The returned slice is shared and MUST NOT be mutated.
func (c *BlockCache) Get(segID uint32, kind BlockKind, blockID uint64) ([]byte, bool) {
	key := cacheKey{segID: segID, kind: kind, blockID: blockID}
	s := c.shardFor(key)
	s.mu.Lock()
	val, ok := s.get(key)
	s.mu.Unlock()
	return val, ok
}

// Put caches a flat byte block, evicting least-recently-used blocks as needed
// to stay within the shard budget. Values larger than a shard budget are
// silently not cached (callers fall back to direct reads).
func (c *BlockCache) Put(segID uint32, kind BlockKind, blockID uint64, val []byte) {
	key := cacheKey{segID: segID, kind: kind, blockID: blockID}
	s := c.shardFor(key)
	s.mu.Lock()
	s.put(key, val)
	s.mu.Unlock()
}

// PutValue caches v when it is a flat []byte block; other types are rejected.
func (c *BlockCache) PutValue(segID uint32, kind BlockKind, blockID uint64, v any) error {
	val, ok := v.([]byte)
	if !ok {
		return ErrBlockCacheValue
	}
	key := cacheKey{segID: segID, kind: kind, blockID: blockID}
	s := c.shardFor(key)
	s.mu.Lock()
	accepted := s.put(key, val)
	s.mu.Unlock()
	if !accepted {
		return ErrBlockCacheOversized
	}
	return nil
}

// get returns the value for key and refreshes its LRU position. Caller holds s.mu.
func (s *cacheShard) get(key cacheKey) ([]byte, bool) {
	e, ok := s.entries[key]
	if !ok {
		return nil, false
	}
	s.moveToFront(e)
	return e.val, true
}

// put stores val under key, evicting LRU entries to stay within the shard
// budget. It reports whether the value was cached. Caller holds s.mu.
func (s *cacheShard) put(key cacheKey, val []byte) bool {
	if int64(len(val)) > s.limit {
		return false
	}
	if e, exists := s.entries[key]; exists {
		s.bytes += int64(len(val)) - int64(len(e.val))
		e.val = val
		s.moveToFront(e)
		s.evictLocked()
		return true
	}
	e := &cacheEntry{key: key, val: val}
	s.entries[key] = e
	s.linkFront(e)
	s.bytes += int64(len(val))
	s.evictLocked()
	return true
}

func (s *cacheShard) linkFront(e *cacheEntry) {
	e.prev = &s.head
	e.next = s.head.next
	s.head.next.prev = e
	s.head.next = e
}

func (s *cacheShard) unlink(e *cacheEntry) {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev = nil
	e.next = nil
}

func (s *cacheShard) moveToFront(e *cacheEntry) {
	if s.head.next == e {
		return
	}
	s.unlink(e)
	s.linkFront(e)
}

// evictLocked drops LRU entries until the shard is within its budget.
func (s *cacheShard) evictLocked() {
	for s.bytes > s.limit {
		lru := s.head.prev
		if lru == &s.head {
			s.bytes = 0
			return
		}
		s.unlink(lru)
		delete(s.entries, lru.key)
		s.bytes -= int64(len(lru.val))
	}
}
