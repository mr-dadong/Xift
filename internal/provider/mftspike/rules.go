package mftspike

import "strings"

// FilterAction defines the 3-level noise governance decision for a directory or file.
type FilterAction uint8

const (
	ActionKeep        FilterAction = iota
	ActionHardExclude              // Level 0: drop completely (including subtree for dirs)
	ActionFold                     // Level 1: keep dir shell with FlagFolded, drop all descendants
	ActionSoftDemote               // Level 2: keep with FlagSystem, propagate FlagSystem to descendants
)

// RuleSet classifies directory and file names according to Xift's 3-level noise governance model.
type RuleSet struct {
	hardExcludeDirs map[string]struct{}
	hardExcludeExts map[string]struct{}
	foldDirs        map[string]struct{}
	softDemoteDirs  map[string]struct{}
}

// DefaultRules returns Xift's built-in cross-platform + Windows 3-level pruning presets.
func DefaultRules() *RuleSet {
	return &RuleSet{
		hardExcludeDirs: toLowerSet(
			"$recycle.bin",
			"system volume information",
			".trash",
			"cache",
			"cache_data",
			"code cache",
			"gpucache",
			"grshadercache",
			"dawncache",
			"shadercache",
			"service worker",
			"temp",
			"tmp",
			"__pycache__",
			".pytest_cache",
			".mypy_cache",
			".ruff_cache",
			"overlay2",
			"crashpad",
			"crashreports",
			"inetsrv",
			"winsxs",
			"softwaredistribution",
			"package cache",
			"cacheddata",
			"cachedextensions",
			"cachedextensionvsixs",
			"blob_storage",
			"indexeddb",
			"inetcache",
			"webcache",
			"wintemp",
		),
		hardExcludeExts: toLowerSet(
			".tmp",
			".temp",
			".bak",
			".swp",
			".etl",
			".dmp",
		),
		foldDirs: toLowerSet(
			"node_modules",
			".git",
			".hg",
			".svn",
			".venv",
			"venv",
			"target",
			"dist",
			"build",
			".next",
			".nuxt",
			".turbo",
			".parcel-cache",
			".cache",
			"go-build",
			"vendor",
			"pods",
			".gradle",
			".cargo",
			".npm",
			".pnpm-store",
			".yarn",
			".nuget",
			"obj",
			"packages",
			"__macosx",
			"site-packages",
		),
		softDemoteDirs: toLowerSet(
			"windows",
			"program files",
			"program files (x86)",
			"programdata",
			"appdata",
			"perflogs",
			"$windows.~bt",
			"$windows.~ws",
		),
	}
}

// ClassifyDir returns the governance action for a directory name.
func (r *RuleSet) ClassifyDir(name string) FilterAction {
	if name == "" {
		return ActionKeep
	}
	lower := strings.ToLower(name)
	if _, ok := r.hardExcludeDirs[lower]; ok {
		return ActionHardExclude
	}
	if _, ok := r.foldDirs[lower]; ok {
		return ActionFold
	}
	if _, ok := r.softDemoteDirs[lower]; ok {
		return ActionSoftDemote
	}
	return ActionKeep
}

// ClassifyFile returns the governance action for a file name.
func (r *RuleSet) ClassifyFile(name string) FilterAction {
	if name == "" {
		return ActionHardExclude
	}
	// NTFS internal meta-files ($MFT, $LogFile, $UsnJrnl, etc.)
	if name[0] == '$' {
		return ActionHardExclude
	}
	if dot := strings.LastIndexByte(name, '.'); dot >= 0 && dot < len(name)-1 {
		ext := strings.ToLower(name[dot:])
		if _, ok := r.hardExcludeExts[ext]; ok {
			return ActionHardExclude
		}
	}
	return ActionKeep
}

func toLowerSet(items ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(items))
	for _, s := range items {
		m[strings.ToLower(s)] = struct{}{}
	}
	return m
}
