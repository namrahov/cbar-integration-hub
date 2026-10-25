# Helpers shared by infra scripts. Dot-source: . "$PSScriptRoot\..\common.ps1"

# Random alphanumeric secret (no quotes or shell metacharacters, safe in JAAS and .env files)
function New-Secret([int]$Length = 24) {
    $chars = [char[]]'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789'
    $bytes = New-Object byte[] $Length
    [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    -join ($bytes | ForEach-Object { $chars[$_ % $chars.Length] })
}

# KEY=VALUE file -> hashtable
function Read-Secrets([string]$Path) {
    $s = @{}
    Get-Content $Path | Where-Object { $_ -match '=' } | ForEach-Object {
        $k, $v = $_ -split '=', 2; $s[$k.Trim()] = $v.Trim()
    }
    $s
}

# Makes sure every key exists in the git-ignored secrets file, generating missing ones.
# Existing values are never changed, so re-running is safe.
function Initialize-Secrets([string]$Path, [string[]]$Keys) {
    $s = @{}
    if (Test-Path $Path) { $s = Read-Secrets $Path }
    $missing = @($Keys | Where-Object { -not $s.ContainsKey($_) })
    if ($missing.Count -gt 0) {
        $missing | ForEach-Object { Add-Content -Path $Path -Encoding ASCII -Value "$_=$(New-Secret)" }
        Write-Host "Generated $($missing -join ', ') in $Path"
        $s = Read-Secrets $Path
    }
    $s
}

# LF-only file for use inside Linux VMs
function Write-LfFile([string]$Path, [string[]]$Lines) {
    [IO.File]::WriteAllText($Path, (($Lines -join "`n") + "`n"))
}
