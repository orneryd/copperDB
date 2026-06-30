#!/bin/bash
# Build llama.cpp shared library for copperDB local embeddings (Linux/macOS)
#
# Matches NornicDB's scripts/build-llama.sh — clones llama.cpp at the
# expected version, builds with CMake, and places the shared library in lib/llama/.
#
# Usage:
#   ./scripts/build-llama.sh              # CPU-only (default)
#   ./scripts/build-llama.sh --cuda       # CUDA GPU acceleration
#   ./scripts/build-llama.sh --clean      # Force clean rebuild
#
# Output:
#   lib/llama/libllama.so    (Linux)
#   lib/llama/libllama.dylib (macOS)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTDIR="$PROJECT_ROOT/lib/llama"
VERSION_FILE="$OUTDIR/VERSION"
EXPECTED_VERSION="$(tr -d '[:space:]' < "$VERSION_FILE")"
TMPDIR="/tmp/llama-cpp-build-copper-$$"

WITH_CUDA=false
CLEAN=false
for arg in "$@"; do
    case "$arg" in
        --cuda) WITH_CUDA=true ;;
        --clean) CLEAN=true ;;
    esac
done

echo "llama.cpp build for copperDB"
echo "  Version: $EXPECTED_VERSION"
echo "  Output:  $OUTDIR"
if [ "$WITH_CUDA" = true ]; then
    echo "  Backend: CUDA (GPU)"
else
    echo "  Backend: CPU"
fi

# ── Detect platform ───────────────────────────────────────────────────────────
case "$(uname -s)" in
    Linux)  LIB_NAME="libllama.so" ;;
    Darwin) LIB_NAME="libllama.dylib" ;;
    *)      echo "Unsupported OS: $(uname -s)"; exit 1 ;;
esac

# ── Check for pre-built library ───────────────────────────────────────────────
STAMP="$OUTDIR/.version-$EXPECTED_VERSION"
DLL_PATH="$OUTDIR/$LIB_NAME"
if [ "$CLEAN" = false ] && [ -f "$DLL_PATH" ] && [ -f "$STAMP" ]; then
    echo "Already built at $EXPECTED_VERSION (remove $STAMP to rebuild)"
    exit 0
fi

# ── Clone llama.cpp ───────────────────────────────────────────────────────────
rm -rf "$TMPDIR"
echo "Cloning llama.cpp @ $EXPECTED_VERSION..."

if ! git clone --depth 1 --branch "$EXPECTED_VERSION" https://github.com/ggerganov/llama.cpp.git "$TMPDIR" 2>/dev/null; then
    echo "Shallow clone failed, trying full clone..."
    git clone https://github.com/ggerganov/llama.cpp.git "$TMPDIR"
    (cd "$TMPDIR" && git checkout "$EXPECTED_VERSION")
fi

# ── Build with CMake ──────────────────────────────────────────────────────────
BUILDDIR="$TMPDIR/build-copper"
rm -rf "$BUILDDIR"
mkdir -p "$BUILDDIR"

CMAKE_ARGS=(
    -DBUILD_SHARED_LIBS=ON
    -DLLAMA_BUILD_TESTS=OFF
    -DLLAMA_BUILD_EXAMPLES=OFF
    -DLLAMA_BUILD_SERVER=OFF
    -DLLAMA_CURL=OFF
)

if [ "$WITH_CUDA" = true ]; then
    CMAKE_ARGS+=(-DGGML_CUDA=ON)
fi

# macOS: enable Metal acceleration automatically
if [ "$(uname -s)" = "Darwin" ]; then
    CMAKE_ARGS+=(-DGGML_METAL=ON)
    CMAKE_ARGS+=(-DGGML_METAL_EMBED_LIBRARY=ON)
fi

cd "$BUILDDIR"
echo "Running CMake..."
cmake "${CMAKE_ARGS[@]}" .. 2>&1

echo "Building..."
cmake --build . --config Release -j "$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)" 2>&1

cd "$PROJECT_ROOT"

# ── Copy outputs ──────────────────────────────────────────────────────────────
mkdir -p "$OUTDIR"

# Find the built shared library
BUILT_LIB=$(find "$TMPDIR" -name "libllama.so" -o -name "libllama.dylib" | head -1)
if [ -z "$BUILT_LIB" ]; then
    # Try alternative names
    BUILT_LIB=$(find "$TMPDIR" -name "libllama.*" -type f | head -1)
fi

if [ -n "$BUILT_LIB" ]; then
    cp "$BUILT_LIB" "$DLL_PATH"
    echo "Copied $BUILT_LIB -> $DLL_PATH"
else
    echo "ERROR: Could not find built shared library in $TMPDIR"
    find "$TMPDIR" -name "*.so" -o -name "*.dylib"
    exit 1
fi

# Version stamp
touch "$STAMP"

echo ""
echo "llama.cpp $EXPECTED_VERSION built successfully"
echo "  Library: $DLL_PATH"
ls -lh "$DLL_PATH"
