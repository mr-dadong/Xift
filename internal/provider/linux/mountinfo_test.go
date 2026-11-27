package linux

import "testing"

func TestParseMountInfoAndFiltering(t *testing.T) {
	sample := `
22 1 0:21 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw
23 1 0:22 / /sys rw,nosuid,nodev,noexec,relatime - sysfs sysfs rw
24 1 0:5 / /dev rw,nosuid,relatime - devtmpfs udev rw
25 1 0:23 / /run rw,nosuid,nodev - tmpfs tmpfs rw
26 1 259:2 / / rw,relatime - ext4 /dev/nvme0n1p2 rw
27 26 259:3 / /mnt/My\040Data rw,relatime - btrfs /dev/nvme1n1p1 rw
28 26 0:45 / /var/lib/docker/overlay2/abc/merged rw - overlay overlay rw
29 26 0:50 / /mnt/nas rw,relatime - nfs4 10.0.0.1:/export rw
30 26 0:51 / /mnt/winshare rw - cifs //10.0.0.2/share rw
31 26 0:52 / /mnt/remote_ssh rw - fuse.sshfs user@host:/data rw
32 26 0:53 / /sys/fs/cgroup rw - cgroup2 cgroup2 rw
`
	mounts := ParseMountInfo(sample)
	if len(mounts) != 11 {
		t.Fatalf("parsed %d mounts, want 11", len(mounts))
	}

	// Verify octal escape decoding (\040 -> space).
	if mounts[5].MountPoint != "/mnt/My Data" || mounts[5].FSType != "btrfs" {
		t.Fatalf("mount[5] = %+v, want /mnt/My Data (btrfs)", mounts[5])
	}

	// Default filter: skips virtual FS AND remote/FUSE mounts.
	localOnly := FilterIndexableMounts(mounts, false)
	if len(localOnly) != 2 {
		t.Fatalf("localOnly = %d (%+v), want 2 (/, /mnt/My Data)", len(localOnly), localOnly)
	}
	if localOnly[0].MountPoint != "/" || localOnly[1].MountPoint != "/mnt/My Data" {
		t.Fatalf("unexpected localOnly mounts: %+v", localOnly)
	}

	// IncludeRemoteMounts=true keeps nfs4/cifs/fuse.sshfs while STILL skipping virtual FS.
	withRemote := FilterIndexableMounts(mounts, true)
	if len(withRemote) != 5 {
		t.Fatalf("withRemote = %d, want 5 (2 local + 3 remote)", len(withRemote))
	}

	// I9 regression: bare "fuse" must also be classified as remote/FUSE.
	if !IsRemoteOrFUSEFSType("fuse") {
		t.Fatal("IsRemoteOrFUSEFSType(\"fuse\") = false, want true")
	}
}
