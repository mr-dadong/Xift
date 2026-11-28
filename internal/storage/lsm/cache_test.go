package lsm

import (
	"reflect"
	"sync"
	"testing"
)

// cacheValueKind enumerates the block kinds stored in the block cache.
func TestBlockCacheBasic(t *testing.T) {
	c := NewBlockCache(1 << 20) // 1 MB
	defer c.Close()

	v1 := []byte("hello-block-1")
	c.Put(1, BlockKindNames, 10, v1)

	got, ok := c.Get(1, BlockKindNames, 10)
	if !ok || string(got) != string(v1) {
		t.Fatalf("Get after Put = %q ok=%v", got, ok)
	}

	// Unknown key misses.
	if _, ok := c.Get(1, BlockKindNames, 11); ok {
		t.Fatal("unexpected hit for missing key")
	}
	// Same blockID but different kind or segment misses.
	if _, ok := c.Get(1, BlockKindInverted, 10); ok {
		t.Fatal("kind must be part of the key")
	}
	if _, ok := c.Get(2, BlockKindNames, 10); ok {
		t.Fatal("segment must be part of the key")
	}
}

func TestCacheShardLRUEviction(t *testing.T) {
	// Deterministic shard-level LRU: budget fits three 1 KB blocks.
	var s cacheShard
	s.init(3 * 1024)

	k1 := cacheKey{segID: 1, kind: BlockKindNames, blockID: 1}
	k2 := cacheKey{segID: 1, kind: BlockKindNames, blockID: 2}
	k3 := cacheKey{segID: 1, kind: BlockKindNames, blockID: 3}
	k4 := cacheKey{segID: 1, kind: BlockKindNames, blockID: 4}

	s.put(k1, make([]byte, 1024))
	s.put(k2, make([]byte, 1024))
	s.put(k3, make([]byte, 1024))

	if _, ok := s.get(k1); !ok {
		t.Fatal("k1 must be resident within budget")
	}
	if s.bytes != 3*1024 {
		t.Fatalf("shard bytes = %d, want %d", s.bytes, 3*1024)
	}

	// Inserting a fourth block evicts the least recently used (k2, because
	// k1 was just touched).
	if !s.put(k4, make([]byte, 1024)) {
		t.Fatal("k4 must be accepted within budget")
	}
	if _, ok := s.get(k2); ok {
		t.Fatal("LRU k2 must have been evicted")
	}
	if _, ok := s.get(k1); !ok {
		t.Fatal("recently used k1 must survive")
	}
	if s.bytes != 3*1024 {
		t.Fatalf("shard bytes after eviction = %d, want %d", s.bytes, 3*1024)
	}
}

func TestCacheShardRejectsOversized(t *testing.T) {
	var s cacheShard
	s.init(1024)
	if s.put(cacheKey{segID: 1, kind: BlockKindInverted, blockID: 1}, make([]byte, 2048)) {
		t.Fatal("oversized value must be rejected by shard")
	}
	if s.bytes != 0 {
		t.Fatalf("bytes = %d after rejection, want 0", s.bytes)
	}
}

func TestBlockCacheBudgetNeverExceeded(t *testing.T) {
	// 64 KB total -> 1 KB per shard; 1 KB blocks churn heavily.
	c := NewBlockCache(64 * 1024)
	defer c.Close()

	for i := uint64(0); i < 200; i++ {
		c.Put(1, BlockKindInverted, i, make([]byte, 1024))
		if got := c.Bytes(); got > 64*1024 {
			t.Fatalf("iteration %d: bytes %d exceeds budget", i, got)
		}
	}
}

func TestBlockCacheZeroMeansDefault(t *testing.T) {
	c := NewBlockCache(0)
	defer c.Close()
	if c.Limit() != DefaultBlockCacheBytes {
		t.Fatalf("Limit() = %d, want default %d", c.Limit(), DefaultBlockCacheBytes)
	}
	c2 := NewBlockCache(32 << 20)
	defer c2.Close()
	if c2.Limit() != 32<<20 {
		t.Fatalf("custom limit = %d, want %d", c2.Limit(), 32<<20)
	}
}

func TestBlockCacheValueMustBeFlatBytes(t *testing.T) {
	c := NewBlockCache(1 << 20)
	defer c.Close()

	// []byte values are noscan; a value carrying Go pointers must be rejected.
	type bad struct{ p *int }
	x := 5
	badVal := bad{p: &x}
	if err := c.PutValue(1, BlockKindNames, 1, badVal); err == nil {
		t.Fatal("pointer-carrying value must be rejected")
	}
	if err := c.PutValue(1, BlockKindNames, 1, []byte("fine")); err != nil {
		t.Fatalf("[]byte value must be accepted: %v", err)
	}
}

func TestBlockCacheConcurrent(t *testing.T) {
	c := NewBlockCache(256 * 1024)
	defer c.Close()

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := uint64(0); i < 500; i++ {
				key := (uint64(w)*97 + i) % 64
				if i%3 == 0 {
					c.Put(1, BlockKindNames, key, make([]byte, 512))
				} else {
					c.Get(1, BlockKindNames, key)
					c.Bytes()
				}
			}
		}(w)
	}
	wg.Wait()
	if got := c.Bytes(); got > 256*1024 {
		t.Fatalf("bytes = %d exceeds budget after concurrency", got)
	}
}

func TestBlockCacheOversizedValueRejected(t *testing.T) {
	c := NewBlockCache(4096)
	defer c.Close()
	// A value larger than the whole budget cannot be cached.
	if err := c.PutValue(1, BlockKindNames, 1, make([]byte, 8192)); err == nil {
		t.Fatal("oversized value must be rejected")
	}
	// A value larger than one shard budget (limit/64) but within total must
	// still be rejected to keep per-shard accounting simple and bounded.
	large := make([]byte, c.Limit()/32)
	if err := c.PutValue(1, BlockKindNames, 2, large); err == nil {
		t.Fatal("value larger than shard budget must be rejected")
	}
}

func TestBlockCacheValueKindIsFlatBytes(t *testing.T) {
	// Cached values must be flat byte blocks (noscan payloads): the accepted
	// value type is []byte whose element kind is uint8 with no pointers.
	typ := reflect.TypeOf([]byte{})
	if typ.Kind() != reflect.Slice || typ.Elem().Kind() != reflect.Uint8 {
		t.Fatalf("value type = %v, want []byte", typ)
	}

	c := NewBlockCache(1 << 16)
	defer c.Close()
	if err := c.PutValue(7, BlockKindNames, 3, []byte("flat")); err != nil {
		t.Fatalf("flat value must be accepted: %v", err)
	}
	if v, ok := c.Get(7, BlockKindNames, 3); !ok || string(v) != "flat" {
		t.Fatalf("Get = %q ok=%v", v, ok)
	}
}
