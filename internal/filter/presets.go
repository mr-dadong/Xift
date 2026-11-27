package filter

import "runtime"

// ProfileID identifies a toggleable built-in cross-platform noise governance profile.
type ProfileID string

const (
	// ProfileSystemBrowserCache hard-excludes OS temp, recycle bins, browser caches, and temp file extensions.
	ProfileSystemBrowserCache ProfileID = "system-browser-cache"
	// ProfileWebNode folds JavaScript/TypeScript dependency and frontend build caches.
	ProfileWebNode ProfileID = "web-node"
	// ProfileRustGoJavaCpp folds VCS internals and compiled language dependency/build directories.
	ProfileRustGoJavaCpp ProfileID = "rust-go-java-cpp"
	// ProfilePythonAI hard-excludes Python caches and folds virtualenvs / AI model caches.
	ProfilePythonAI ProfileID = "python-ai"
	// ProfileMediaRenderCache hard-excludes video/audio editor preview and render cache directories.
	ProfileMediaRenderCache ProfileID = "media-render-cache"
	// ProfileOSSystem soft-demotes OS installation and system library directories.
	ProfileOSSystem ProfileID = "os-system"
)

// Profile describes a built-in noise governance rule bundle.
type Profile struct {
	ID              ProfileID
	Name            string
	DefaultEnabled  bool
	HardExcludeDirs []string
	HardExcludeExts []string
	FoldDirs        []string
	// SoftDemoteDirs matches at any depth (e.g. AppData).
	SoftDemoteDirs []string
	// SoftDemoteRootDirs matches only top-level directories directly under a volume root (depth == 1),
	// preventing false positives on deep user project directories like projects/myapp/lib.
	SoftDemoteRootDirs []string
}

// DefaultCaseInsensitive returns the platform default filename case sensitivity policy.
// Windows and macOS default to case-insensitive; Linux defaults to case-sensitive.
func DefaultCaseInsensitive() bool {
	return runtime.GOOS == "windows" || runtime.GOOS == "darwin"
}

// BuiltInProfiles returns copies of all 6 built-in cross-platform rule profiles.
func BuiltInProfiles() []Profile {
	return []Profile{
		{
			ID:             ProfileSystemBrowserCache,
			Name:           "System & Browser Caches",
			DefaultEnabled: true,
			HardExcludeDirs: []string{
				"$Recycle.Bin",
				"System Volume Information",
				".Trash",
				".Trashes",
				".fseventsd",
				".Spotlight-V100",
				"Cache",
				"Cache_Data",
				"Code Cache",
				"GPUCache",
				"GrShaderCache",
				"DawnCache",
				"ShaderCache",
				"Service Worker",
				"Temp",
				"tmp",
				"WinTemp",
				"overlay2",
				"Crashpad",
				"CrashReports",
				"inetsrv",
				"WinSxS",
				"SoftwareDistribution",
				"Package Cache",
				"CachedData",
				"CachedExtensions",
				"CachedExtensionVSIXs",
				"blob_storage",
				"IndexedDB",
				"INetCache",
				"WebCache",
			},
			HardExcludeExts: []string{
				".tmp",
				".temp",
				".bak",
				".swp",
				".swo",
				".etl",
				".dmp",
			},
		},
		{
			ID:             ProfileWebNode,
			Name:           "Web / Node.js",
			DefaultEnabled: true,
			FoldDirs: []string{
				"node_modules",
				".next",
				".nuxt",
				".turbo",
				".parcel-cache",
				".svelte-kit",
				".vite",
				".npm",
				".pnpm-store",
				".yarn",
				"dist",
				"build",
			},
		},
		{
			ID:             ProfileRustGoJavaCpp,
			Name:           "Rust / Go / Java / C++",
			DefaultEnabled: true,
			FoldDirs: []string{
				".git",
				".hg",
				".svn",
				"target",
				"vendor",
				"go-build",
				"Pods",
				".gradle",
				".cargo",
				".nuget",
				".cache",
				"obj",
				"Packages",
				"cmake-build-debug",
				"cmake-build-release",
				"DerivedData",
			},
		},
		{
			ID:             ProfilePythonAI,
			Name:           "Python / AI",
			DefaultEnabled: true,
			HardExcludeDirs: []string{
				"__pycache__",
				".pytest_cache",
				".mypy_cache",
				".ruff_cache",
				".tox",
			},
			FoldDirs: []string{
				".venv",
				"venv",
				".conda",
				"site-packages",
				".huggingface",
			},
		},
		{
			ID:             ProfileMediaRenderCache,
			Name:           "Media & Render Caches",
			DefaultEnabled: true,
			HardExcludeDirs: []string{
				"Adobe Premiere Pro Video Previews",
				"Adobe Premiere Pro Auto-Save",
				"Media Cache Files",
				"Media Cache",
				"Peak Files",
				"CacheClip",
				"Render Files",
				"__MACOSX",
			},
		},
		{
			ID:             ProfileOSSystem,
			Name:           "OS System Directories",
			DefaultEnabled: true,
			SoftDemoteDirs: []string{
				"AppData",
				"$Windows.~BT",
				"$Windows.~WS",
			},
			SoftDemoteRootDirs: []string{
				// Windows volume root dirs
				"Windows",
				"Program Files",
				"Program Files (x86)",
				"ProgramData",
				"PerfLogs",
				// macOS volume root dirs
				"System",
				"Library",
				"Applications",
				"private",
				// Linux root filesystem dirs
				"usr",
				"lib",
				"lib64",
				"var",
				"etc",
				"opt",
				"boot",
				"snap",
			},
		},
	}
}
