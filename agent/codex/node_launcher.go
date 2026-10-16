package codex

// Fork-only: never let an npm/Node launcher be the process cc-connect owns.
//
// Upstream resolves `cmd` (or PATH, when cmd is unset) to one executable and
// stops there. When that executable happens to be @openai/codex's bundled
// `bin/codex.js` — a `#!/usr/bin/env node` script that spawns the real binary
// as its own child — cc-connect ends up owning an extra `node` process per
// agent session. That shim buys us nothing, and it makes teardown worse than
// useless: killing it only kills the launcher, so the real app-server survives
// as an orphan still holding CODEX_HOME/thread-writer-locks/<threadId>.lock,
// and the next thread/resume fails until someone kills it by hand.
//
// unwrapCodexNodeLauncher walks from such a launcher back to the native binary
// its package vendors next to it, so a project without an explicit `cmd` still
// runs the real thing. It is purely additive: an already-native executable, a
// launcher that is not the Codex npm package, and a package whose vendored
// binary cannot be found all come back unchanged, and the caller keeps the path
// it had. Upstream's resolution order is untouched.

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// codexNativeTriple mirrors the platform table in @openai/codex's bin/codex.js:
// it is what tells us which `vendor/<triple>/` tree the platform package ships.
func codexNativeTriple(goos, goarch string) string {
	switch goos + "/" + goarch {
	case "linux/amd64", "android/amd64":
		return "x86_64-unknown-linux-musl"
	case "linux/arm64", "android/arm64":
		return "aarch64-unknown-linux-musl"
	case "darwin/amd64":
		return "x86_64-apple-darwin"
	case "darwin/arm64":
		return "aarch64-apple-darwin"
	case "windows/amd64":
		return "x86_64-pc-windows-msvc"
	case "windows/arm64":
		return "aarch64-pc-windows-msvc"
	default:
		return ""
	}
}

// unwrapCodexNodeLauncher returns the native Codex binary behind an npm
// launcher, plus whether the swap should happen at all.
func unwrapCodexNodeLauncher(path string) (string, bool) {
	if strings.TrimSpace(path) == "" {
		return "", false
	}
	real := path
	if evaluated, err := filepath.EvalSymlinks(path); err == nil {
		real = evaluated
	}
	// A real ELF/Mach-O/PE is what we want already: no reason to second-guess
	// an operator who configured one directly.
	if launcherIsNative(real) {
		return "", false
	}
	triple := codexNativeTriple(runtime.GOOS, runtime.GOARCH)
	if triple == "" {
		return "", false
	}
	root, ok := codexPackageRoot(filepath.Dir(real))
	if !ok {
		return "", false
	}
	found := findVendoredCodexBinary(root, triple)
	if found == "" {
		return "", false
	}
	return found, true
}

// launcherIsNative reports whether path is a compiled executable rather than a
// script (which is how every npm launcher starts life: `#!/usr/bin/env node`).
func launcherIsNative(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = file.Close() }()
	var header [4]byte
	if _, err := io.ReadFull(file, header[:]); err != nil {
		return false
	}
	if header[0] == 0x7f && header[1] == 'E' && header[2] == 'L' && header[3] == 'F' {
		return true
	}
	if header[0] == 'M' && header[1] == 'Z' {
		return true
	}
	// Mach-O: thin (feedface/feedfacf) and universal (cafebabe/cafebabf).
	switch binary.BigEndian.Uint32(header[:]) {
	case 0xfeedface, 0xfeedfacf, 0xcafebabe, 0xcafebabf:
		return true
	default:
		return false
	}
}

// codexPackageRoot walks up from a launcher's directory to the directory that
// holds @openai/codex's package.json. The platform packages reuse the same
// `name`, so a launcher that lives inside one resolves to that package.
func codexPackageRoot(dir string) (string, bool) {
	for hop := 0; hop < 4 && dir != "" && dir != string(filepath.Separator); hop++ {
		if packageJSONNamesCodex(filepath.Join(dir, "package.json")) {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", false
}

func packageJSONNamesCodex(path string) bool {
	raw, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var manifest struct {
		Name                 string            `json:"name"`
		OptionalDependencies map[string]string `json:"optionalDependencies"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return false
	}
	if manifest.Name == "@openai/codex" {
		return true
	}
	for dependency := range manifest.OptionalDependencies {
		if strings.HasPrefix(dependency, "@openai/codex-") {
			return true
		}
	}
	return false
}

// findVendoredCodexBinary looks for `vendor/<triple>/bin/codex` under the
// package's own node_modules, under a hoisted parent node_modules, and finally
// anywhere shallow inside the package (the platform package's layout).
func findVendoredCodexBinary(root, triple string) string {
	binaryName := "codex"
	if runtime.GOOS == "windows" {
		binaryName = "codex.exe"
	}
	roots := []string{filepath.Join(root, "node_modules")}
	if parent := filepath.Dir(root); parent != "" && parent != root {
		roots = append(roots, filepath.Join(parent, "node_modules"))
	}
	for _, searchRoot := range roots {
		if found := searchVendorTree(searchRoot, triple, binaryName, 5); found != "" {
			return found
		}
	}
	return searchVendorTree(root, triple, binaryName, 4)
}

// vendorTreeSkipDirs keeps the walk away from the two packaged resource trees
// (thousands of files) and from anything else that cannot hold the binary.
var vendorTreeSkipDirs = map[string]bool{
	"codex-resources": true,
	"codex-path":      true,
	".git":            true,
	"docs":            true,
	"assets":          true,
	"node_modules":    true, // handled by the explicit search roots above
}

func searchVendorTree(root, triple, binaryName string, maxDepth int) string {
	suffix := "/" + filepath.ToSlash(filepath.Join("vendor", triple, "bin"))
	var found string
	var walk func(dir string, depth int)
	walk = func(dir string, depth int) {
		if found != "" || depth > maxDepth {
			return
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		for _, entry := range entries {
			if found != "" {
				return
			}
			if !entry.IsDir() {
				continue
			}
			name := entry.Name()
			full := filepath.Join(dir, name)
			if strings.HasSuffix(filepath.ToSlash(full), suffix) {
				candidate := filepath.Join(full, binaryName)
				if launcherIsNative(candidate) && isExecutableFile(candidate) {
					found = candidate
					return
				}
				continue
			}
			if vendorTreeSkipDirs[name] {
				continue
			}
			walk(full, depth+1)
		}
	}
	walk(root, 0)
	return found
}

func isExecutableFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	if runtime.GOOS == "windows" {
		return true
	}
	return info.Mode().Perm()&0o111 != 0
}
