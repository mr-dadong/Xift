package platform

import (
	"context"
	"testing"

	"github.com/mr-dadong/Xift/internal/filter"
	"github.com/mr-dadong/Xift/internal/provider"
)

func TestNewDefaultProviderSmoke(t *testing.T) {
	p, err := NewDefaultProvider(filter.NewPruner(nil, nil), provider.Options{DisableLowPriority: true})
	if err != nil {
		t.Fatalf("NewDefaultProvider: %v", err)
	}
	if p == nil {
		t.Fatal("NewDefaultProvider returned nil")
	}
	vols, err := p.EnumerateVolumes(context.Background())
	if err != nil {
		t.Fatalf("EnumerateVolumes: %v", err)
	}
	if len(vols) == 0 {
		t.Fatal("expected at least one volume from default platform provider")
	}
}
