package linux

import (
	"os"
	"strconv"
	"strings"
)

// MountEntry represents one parsed line from /proc/self/mountinfo.
type MountEntry struct {
	MountID    int
	ParentID   int
	Major      int
	Minor      int
	Root       string
	MountPoint string
	Options    string
	FSType     string
	Source     string
}

var virtualFSTypes = map[string]struct{}{
	"proc":            {},
	"sysfs":           {},
	"devtmpfs":        {},
	"devpts":          {},
	"tmpfs":           {},
	"overlay":         {},
	"cgroup":          {},
	"cgroup2":         {},
	"pstore":          {},
	"securityfs":      {},
	"debugfs":         {},
	"tracefs":         {},
	"configfs":        {},
	"autofs":          {},
	"mqueue":          {},
	"hugetlbfs":       {},
	"bpf":             {},
	"nsfs":            {},
	"efivarfs":        {},
	"squashfs":        {},
	"fusectl":         {},
	"rpc_pipefs":      {},
	"binfmt_misc":     {},
	"selinuxfs":       {},
	"ramfs":           {},
	"fuse.portal":     {},
	"fuse.gvfsd-fuse": {},
}

var remoteFSTypes = map[string]struct{}{
	"nfs":         {},
	"nfs4":        {},
	"cifs":        {},
	"smb":         {},
	"smb3":        {},
	"smbfs":       {},
	"9p":          {},
	"afs":         {},
	"ceph":        {},
	"glusterfs":   {},
	"sshfs":       {},
	"davfs":       {},
	"fuse.sshfs":  {},
	"fuse.s3fs":   {},
	"fuse.rclone": {},
}

// ParseMountInfo parses the contents of /proc/self/mountinfo, decoding octal escapes (\040 -> space).
func ParseMountInfo(content string) []MountEntry {
	var out []MountEntry
	for _, rawLine := range strings.Split(content, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}
		sepIdx := -1
		for i := 6; i < len(fields); i++ {
			if fields[i] == "-" {
				sepIdx = i
				break
			}
		}
		if sepIdx < 0 || sepIdx+2 >= len(fields) {
			continue
		}

		mountID, _ := strconv.Atoi(fields[0])
		parentID, _ := strconv.Atoi(fields[1])
		major, minor := parseMajorMinor(fields[2])

		out = append(out, MountEntry{
			MountID:    mountID,
			ParentID:   parentID,
			Major:      major,
			Minor:      minor,
			Root:       unescapeMountField(fields[3]),
			MountPoint: unescapeMountField(fields[4]),
			Options:    fields[5],
			FSType:     fields[sepIdx+1],
			Source:     unescapeMountField(fields[sepIdx+2]),
		})
	}
	return out
}

// ReadSelfMountInfo reads and parses /proc/self/mountinfo strictly read-only.
func ReadSelfMountInfo() ([]MountEntry, error) {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	return ParseMountInfo(string(data)), nil
}

// IsVirtualFSType reports whether fsType is a pseudo/kernel virtual filesystem that must never be indexed.
func IsVirtualFSType(fsType string) bool {
	_, ok := virtualFSTypes[strings.ToLower(fsType)]
	return ok
}

// IsRemoteOrFUSEFSType reports whether fsType is a network/FUSE filesystem skipped by default to avoid hangs.
func IsRemoteOrFUSEFSType(fsType string) bool {
	low := strings.ToLower(fsType)
	if low == "fuse" {
		return true
	}
	if _, ok := remoteFSTypes[low]; ok {
		return true
	}
	if strings.HasPrefix(low, "fuse.") && low != "fuse.ntfs" && low != "fuseblk" {
		return true
	}
	return false
}

// FilterIndexableMounts filters parsed mountinfo entries, always skipping virtual filesystems
// and skipping remote/FUSE mounts unless includeRemote is true.
func FilterIndexableMounts(entries []MountEntry, includeRemote bool) []MountEntry {
	out := make([]MountEntry, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		if e.MountPoint == "" || IsVirtualFSType(e.FSType) {
			continue
		}
		if !includeRemote && IsRemoteOrFUSEFSType(e.FSType) {
			continue
		}
		if _, dup := seen[e.MountPoint]; dup {
			continue
		}
		seen[e.MountPoint] = struct{}{}
		out = append(out, e)
	}
	return out
}

func parseMajorMinor(s string) (int, int) {
	idx := strings.IndexByte(s, ':')
	if idx < 0 {
		return 0, 0
	}
	maj, _ := strconv.Atoi(s[:idx])
	min, _ := strconv.Atoi(s[idx+1:])
	return maj, min
}

func unescapeMountField(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if val, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(val))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
