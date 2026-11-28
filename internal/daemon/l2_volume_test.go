package daemon

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/mr-dadong/Xift/internal/provider"
)

// TestVolumeForPath verifies whitelisted roots resolve to their owning volume
// using path-boundary longest-prefix matching.
func TestVolumeForPath(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "data")
	vols := []provider.VolumeInfo{
		{VolID: 1, RootPath: root},
		{VolID: 2, RootPath: base},
		{VolID: 3, RootPath: filepath.Join(base, "other")},
	}

	cases := []struct {
		name string
		abs  string
		want uint16
		ok   bool
	}{
		{"exact root", root, 1, true},
		{"nested dir", filepath.Join(root, "a", "b"), 1, true},
		{"root with trailing separator", root + string(filepath.Separator), 1, true},
		{"sibling under parent volume", filepath.Join(base, "sibling"), 2, true},
		{"shared name prefix is not a boundary", root + "-copy", 2, true},
		{"other volume", filepath.Join(base, "other", "x"), 3, true},
		{"outside every volume", filepath.Join(filepath.Dir(base), "elsewhere"), 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vol, ok := volumeForPath(vols, tc.abs)
			if ok != tc.ok {
				t.Fatalf("volumeForPath(%q) ok = %v, want %v", tc.abs, ok, tc.ok)
			}
			if ok && vol.VolID != tc.want {
				t.Fatalf("volumeForPath(%q) VolID = %d, want %d", tc.abs, vol.VolID, tc.want)
			}
		})
	}

	if _, ok := volumeForPath(nil, root); ok {
		t.Fatal("empty volume list must not resolve")
	}
	if _, ok := volumeForPath(vols, "relative/path"); ok {
		t.Fatal("relative path must not resolve to a volume")
	}
}

// TestVolumeForPathCaseInsensitive verifies Windows drive paths match without
// regard to letter case.
func TestVolumeForPathCaseInsensitive(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("drive-letter matching is Windows-specific")
	}
	vols := []provider.VolumeInfo{{VolID: 7, RootPath: `C:\`}}
	vol, ok := volumeForPath(vols, `c:\Users\someone`)
	if !ok || vol.VolID != 7 {
		t.Fatalf("volumeForPath mixed-case = (%v, %v), want volume 7", vol.VolID, ok)
	}
	if strings.EqualFold(vol.RootPath, "") {
		t.Fatal("resolved volume lost its root path")
	}
}
