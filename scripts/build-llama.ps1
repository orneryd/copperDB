# Build llama.cpp shared library for copperDB local embeddings (Windows)
#
# Matches NornicDB's scripts/build-llama-cuda.ps1 — clones llama.cpp at the
# expected version, builds with CMake, and places the DLL in lib/llama/.
#
# Requirements:
#   - CMake 3.24+
#   - Git
#   - Visual Studio Build Tools (or MinGW-w64 for GCC)
#
# Usage:
#   .\scripts\build-llama.ps1                # CPU-only (default)
#   .\scripts\build-llama.ps1 -WithCuda      # CUDA GPU acceleration
#   .\scripts\build-llama.ps1 -Clean         # Force clean rebuild
#
# Output:
#   lib\llama\llama.dll                       (shared library)
#   lib\llama\llama.h, ggml.h, ggml-cpu.h     (headers for reference)

param(
    [switch]$WithCuda,
    [switch]$Clean
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$OutDir = Join-Path $ProjectRoot "lib\llama"
$TmpDir = Join-Path $env:TEMP "llama-cpp-build-copper"
$VersionFile = Join-Path $OutDir "VERSION"
$ExpectedVersion = (Get-Content $VersionFile).Trim()

Write-Host "llama.cpp build for copperDB" -ForegroundColor Cyan
Write-Host "  Version: $ExpectedVersion"
Write-Host "  Output:  $OutDir"
if ($WithCuda) {
    Write-Host "  Backend: CUDA (GPU)" -ForegroundColor Green
} else {
    Write-Host "  Backend: CPU" -ForegroundColor Yellow
}

# ── Check for pre-built DLL ───────────────────────────────────────────────────
$DllPath = Join-Path $OutDir "llama.dll"
$StampPath = Join-Path $OutDir ".version-$ExpectedVersion"
if (-not $Clean -and (Test-Path $DllPath) -and (Test-Path $StampPath)) {
    Write-Host "Already built at $ExpectedVersion (remove $StampPath to rebuild)" -ForegroundColor Green
    exit 0
}

# ── Clone llama.cpp ───────────────────────────────────────────────────────────
if (Test-Path $TmpDir) {
    Remove-Item -Recurse -Force $TmpDir
}

Write-Host "Cloning llama.cpp @ $ExpectedVersion..."
git clone --depth 1 --branch $ExpectedVersion https://github.com/ggerganov/llama.cpp.git $TmpDir 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "Shallow clone failed, trying full clone..." -ForegroundColor Yellow
    git clone https://github.com/ggerganov/llama.cpp.git $TmpDir 2>&1
    Push-Location $TmpDir
    git checkout $ExpectedVersion 2>&1
    Pop-Location
}

# ── Build with CMake ──────────────────────────────────────────────────────────
Push-Location $TmpDir
$BuildDir = "build-copper"
if (Test-Path $BuildDir) {
    Remove-Item -Recurse -Force $BuildDir
}
New-Item -ItemType Directory -Force $BuildDir | Out-Null
Push-Location $BuildDir

$cmakeArgs = @(
    "..",
    "-DBUILD_SHARED_LIBS=ON",
    "-DLLAMA_BUILD_TESTS=OFF",
    "-DLLAMA_BUILD_EXAMPLES=OFF",
    "-DLLAMA_BUILD_SERVER=OFF",
    "-DLLAMA_CURL=OFF"
)

if ($WithCuda) {
    $cmakeArgs += "-DGGML_CUDA=ON"
    $cmakeArgs += "-DCMAKE_CUDA_ARCHITECTURES=all"
}

Write-Host "Running CMake..."
& cmake @cmakeArgs 2>&1
if ($LASTEXITCODE -ne 0) { throw "CMake configuration failed" }

Write-Host "Building..."
& cmake --build . --config Release -j $env:NUMBER_OF_PROCESSORS 2>&1
if ($LASTEXITCODE -ne 0) { throw "Build failed" }

Pop-Location  # build-copper
Pop-Location  # llama.cpp

# ── Copy outputs ──────────────────────────────────────────────────────────────
if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force $OutDir | Out-Null
}

$BuiltDll = Join-Path $TmpDir $BuildDir "bin\Release\llama.dll"
if (-not (Test-Path $BuiltDll)) {
    $BuiltDll = Join-Path $TmpDir $BuildDir "bin\llama.dll"
}
if (-not (Test-Path $BuiltDll)) {
    $BuiltDll = Join-Path $TmpDir $BuildDir "src\Release\llama.dll"
}
if (-not (Test-Path $BuiltDll)) {
    $BuiltDll = Join-Path $TmpDir $BuildDir "src\libllama.dll"
}

if (Test-Path $BuiltDll) {
    Copy-Item $BuiltDll $DllPath -Force
    Write-Host "Copied $BuiltDll -> $DllPath" -ForegroundColor Green
} else {
    Write-Host "Searching for built DLL..."
    $found = Get-ChildItem -Path $TmpDir -Recurse -Filter "llama*.dll" -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($found) {
        Copy-Item $found.FullName $DllPath -Force
        Write-Host "Found and copied: $($found.FullName)" -ForegroundColor Green
    } else {
        throw "Could not find built llama.dll anywhere in $TmpDir"
    }
}

# Version stamp
"" | Out-File -FilePath $StampPath -Encoding ascii

Write-Host ""
Write-Host "llama.cpp $ExpectedVersion built successfully" -ForegroundColor Green
Write-Host "  DLL: $DllPath"
Write-Host "  Size: $((Get-Item $DllPath).Length / 1MB) MB"
