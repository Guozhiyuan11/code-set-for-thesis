param(
    [string]$TaskName = "SmartSleeperPipeline"
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$envFile = Join-Path $projectRoot ".env"
if (-not (Test-Path $envFile -PathType Leaf)) {
    throw "Create .env from .env.example and add the source and Supabase credentials first."
}
$envText = Get-Content -Raw $envFile
foreach ($key in @(
    "SMART_SLEEPER_USERNAME",
    "SMART_SLEEPER_PASSWORD",
    "SUPABASE_URL",
    "SUPABASE_SECRET_KEY"
)) {
    if ($envText -notmatch "(?m)^\s*$key\s*=\s*.+$") {
        throw "Missing $key in .env"
    }
}
$pythonCandidates = @(Get-ChildItem `
    -Path (Join-Path $projectRoot ".uv-python") `
    -Filter python.exe `
    -Recurse `
    -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -notmatch "\\Lib\\venv\\" } |
    ForEach-Object { $_.FullName })
$pythonCandidates += (Join-Path $projectRoot ".venv\Scripts\python.exe")
$python = $null
foreach ($candidate in $pythonCandidates) {
    if (-not (Test-Path $candidate -PathType Leaf)) { continue }
    & $candidate -c "import sys" *> $null
    if ($LASTEXITCODE -eq 0) {
        $python = $candidate
        break
    }
}
if (-not $python) {
    throw "No working project Python found. Install Python 3.11 before registering the task."
}
$arguments = "run_pipeline.py --dynamic-config config/dynamic_filtering.json --dynamic-mode shadow"

$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument $arguments `
    -WorkingDirectory $projectRoot
$trigger = New-ScheduledTaskTrigger `
    -Once `
    -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes 15)
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 14)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Fetch, filter, and transfer SMART Sleeper data every 15 minutes" `
    -Force

Write-Host "Installed task '$TaskName'. It will start within one minute."
