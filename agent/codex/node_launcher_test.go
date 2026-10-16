package codex

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// npmCodexLauncherFixture builds the shape an `npm install -g @openai/codex`
// leaves behind: the package's Node launcher, the platform package nested the
// way npm nests it here, and the native binary that launcher would have spawned.
// It returns the launcher path (a symlink, mirroring how nvm puts `codex` on
// PATH) and the native binary path.
func npmCodexLauncherFixture(t *testing.T) (launcher, native string) {
	t.Helper()
	triple := codexNativeTriple(runtime.GOOS, runtime.GOARCH)
	if triple == "" {
		t.Skipf("no vendored layout for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	root := t.TempDir()
	pkg := filepath.Join(root, "lib", "node_modules", "@openai", "codex")
	if err := os.MkdirAll(filepath.Join(pkg, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pkg, "package.json"),
		[]byte(`{"name":"@openai/codex","version":"0.0.0-test","bin":{"codex":"bin/codex.js"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	js := filepath.Join(pkg, "bin", "codex.js")
	if err := os.WriteFile(js, []byte("#!/usr/bin/env node\n// launcher\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	native = filepath.Join(pkg, "node_modules", "@openai", "codex-linux-x64", "vendor", triple, "bin", "codex")
	writeFakeNativeBinary(t, native)

	binDir := filepath.Join(root, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	launcher = filepath.Join(binDir, "codex")
	if err := os.Symlink(js, launcher); err != nil {
		t.Fatal(err)
	}
	return launcher, native
}

func writeFakeNativeBinary(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	// ELF magic is all launcherIsNative looks at.
	if err := os.WriteFile(path, []byte{0x7f, 'E', 'L', 'F', 0x02, 0x01}, 0o755); err != nil {
		t.Fatal(err)
	}
}

func TestCodexNativeTriple(t *testing.T) {
	cases := []struct {
		goos   string
		goarch string
		want   string
	}{
		{"linux", "amd64", "x86_64-unknown-linux-musl"},
		{"linux", "arm64", "aarch64-unknown-linux-musl"},
		{"darwin", "amd64", "x86_64-apple-darwin"},
		{"darwin", "arm64", "aarch64-apple-darwin"},
		{"windows", "amd64", "x86_64-pc-windows-msvc"},
		{"windows", "arm64", "aarch64-pc-windows-msvc"},
		{"freebsd", "amd64", ""},
	}
	for _, tc := range cases {
		if got := codexNativeTriple(tc.goos, tc.goarch); got != tc.want {
			t.Errorf("codexNativeTriple(%q, %q) = %q, want %q", tc.goos, tc.goarch, got, tc.want)
		}
	}
}

// New is the single choke point every spawn path inherits its binary from, so
// that is where the swap has to happen for the npm layout.
func TestNewPinsNativeBinaryInPlaceOfNodeLauncher(t *testing.T) {
	launcher, native := npmCodexLauncherFixture(t)
	agent, err := New(map[string]any{"cmd": launcher})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	resolved, ok := agent.(*Agent)
	if !ok {
		t.Fatalf("unexpected agent type %T", agent)
	}
	wantResolved, err := filepath.EvalSymlinks(native)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.cmd != wantResolved {
		t.Errorf("New kept cmd=%q, want the native binary %q", resolved.cmd, wantResolved)
	}
}

// The fixture is exactly the npm layout, so the launcher must be recognised and
// swapped for the native binary sitting in the platform package.
func TestUnwrapCodexNodeLauncherRewritesNpmShim(t *testing.T) {
	launcher, native := npmCodexLauncherFixture(t)
	got, ok := unwrapCodexNodeLauncher(launcher)
	if !ok {
		t.Fatalf("unwrapCodexNodeLauncher(%q) did not rewrite the npm launcher", launcher)
	}
	wantResolved, err := filepath.EvalSymlinks(native)
	if err != nil {
		t.Fatal(err)
	}
	if got != wantResolved {
		t.Errorf("unwrapCodexNodeLauncher(%q) = %q, want %q", launcher, got, wantResolved)
	}
}

// Anything that is not the Codex npm launcher must survive untouched, otherwise
// the fork would be changing upstream behaviour instead of adding to it.
func TestUnwrapCodexNodeLauncherLeavesOthersAlone(t *testing.T) {
	t.Run("native binary", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "codex")
		writeFakeNativeBinary(t, path)
		if got, ok := unwrapCodexNodeLauncher(path); ok {
			t.Errorf("a real binary must not be rewritten, got %q", got)
		}
	})

	t.Run("non-codex node script", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "package.json"),
			[]byte(`{"name":"some-other-cli","version":"1.0.0"}`), 0o644); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "cli.js")
		if err := os.WriteFile(path, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		if got, ok := unwrapCodexNodeLauncher(path); ok {
			t.Errorf("an unrelated node script must not be rewritten, got %q", got)
		}
	})

	t.Run("missing vendored binary", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "package.json"),
			[]byte(`{"name":"@openai/codex","version":"0.0.0-test"}`), 0o644); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "codex.js")
		if err := os.WriteFile(path, []byte("#!/usr/bin/env node\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		if got, ok := unwrapCodexNodeLauncher(path); ok {
			t.Errorf("without a vendored binary the launcher is all we have, got %q", got)
		}
	})

	t.Run("empty path", func(t *testing.T) {
		if got, ok := unwrapCodexNodeLauncher("   "); ok {
			t.Errorf("blank input must be a no-op, got %q", got)
		}
	})

	t.Run("oversized depth is not walked", func(t *testing.T) {
		// Guards the depth limit: a vendored tree buried too deep should not be
		// found, so the walk never turns into a filesystem tour.
		root := t.TempDir()
		deep := filepath.Join(root, "a", "b", "c", "d", "e", "f")
		native := filepath.Join(deep, "vendor", codexNativeTriple(runtime.GOOS, runtime.GOARCH), "bin", "codex")
		writeFakeNativeBinary(t, native)
		if got := searchVendorTree(root, codexNativeTriple(runtime.GOOS, runtime.GOARCH), "codex", 2); got != "" {
			t.Errorf("depth limit ignored, found %q", got)
		}
	})
}

func TestFindVendoredCodexBinaryIgnoresResourceTrees(t *testing.T) {
	triple := codexNativeTriple(runtime.GOOS, runtime.GOARCH)
	if triple == "" {
		t.Skipf("no vendored layout for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	root := t.TempDir()
	// The packaged resource trees sit next to the binary and are huge; the walk
	// must skip them without losing the real hit.
	writeFakeNativeBinary(t, filepath.Join(root, "codex-resources", "vendor", triple, "bin", "codex"))
	native := filepath.Join(root, "node_modules", "@openai", "codex-linux-x64", "vendor", triple, "bin", "codex")
	writeFakeNativeBinary(t, native)

	if got := findVendoredCodexBinary(root, triple); got != native {
		t.Errorf("findVendoredCodexBinary = %q, want %q", got, native)
	}
}

func TestFindVendoredCodexBinaryNothingOutsideNodeModules(t *testing.T) {
	triple := codexNativeTriple(runtime.GOOS, runtime.GOARCH)
	if triple == "" {
		t.Skipf("no vendored layout for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	root := t.TempDir()
	writeFakeNativeBinary(t, filepath.Join(root, "codex-resources", "vendor", triple, "bin", "codex"))
	if got := findVendoredCodexBinary(root, triple); got != "" {
		t.Errorf("findVendoredCodexBinary should not search the resource trees, got %q", got)
	}
}
