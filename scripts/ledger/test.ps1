param([switch]$RestartDatabase, [switch]$Race)
$ErrorActionPreference = 'Stop'
$ledgerRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
Push-Location $ledgerRoot
try {
    $env:GOPATH = Join-Path $ledgerRoot '.cache/go'
    $env:GOCACHE = Join-Path $ledgerRoot '.cache/go-build'
    $env:GOTMPDIR = Join-Path $ledgerRoot '.cache/go-tmp'
    New-Item -ItemType Directory -Force $env:GOTMPDIR | Out-Null
    $certDir = (Join-Path $ledgerRoot '.cache/ledger-db/certs').Replace('\', '/')
    foreach ($file in @('ca.crt', 'client.root.crt', 'client.root.key', 'client.ledger_runtime.crt', 'client.ledger_runtime.key', 'client.reconciliation_runtime.crt', 'client.reconciliation_runtime.key')) {
        if (-not (Test-Path -LiteralPath "$certDir/$file")) { throw 'Start scripts/ledger/local-db.sh in WSL before running Ledger evidence.' }
    }
    $ca = [Uri]::EscapeDataString("$certDir/ca.crt")
    $env:PESARO_LEDGER_TEST_ADMIN_URL = "postgresql://root@localhost:26277/pesaro_ledger?sslmode=verify-full&sslrootcert=$ca&sslcert=$([Uri]::EscapeDataString("$certDir/client.root.crt"))&sslkey=$([Uri]::EscapeDataString("$certDir/client.root.key"))"
    $env:PESARO_LEDGER_TEST_RUNTIME_URL = "postgresql://ledger_runtime@localhost:26277/pesaro_ledger?sslmode=verify-full&sslrootcert=$ca&sslcert=$([Uri]::EscapeDataString("$certDir/client.ledger_runtime.crt"))&sslkey=$([Uri]::EscapeDataString("$certDir/client.ledger_runtime.key"))"
    $env:PESAR_RECONCILIATION_TEST_ADMIN_URL = $env:PESARO_LEDGER_TEST_ADMIN_URL.Replace('/pesaro_ledger?', '/pesaro_reconciliation?')
    $env:PESAR_RECONCILIATION_TEST_RUNTIME_URL = "postgresql://reconciliation_runtime@localhost:26277/pesaro_reconciliation?sslmode=verify-full&sslrootcert=$ca&sslcert=$([Uri]::EscapeDataString("$certDir/client.reconciliation_runtime.crt"))&sslkey=$([Uri]::EscapeDataString("$certDir/client.reconciliation_runtime.key"))"
    $env:PESAR_LEDGER_EVIDENCE_DIR = Join-Path $ledgerRoot '.cache/ledger-evidence'
    New-Item -ItemType Directory -Force $env:PESAR_LEDGER_EVIDENCE_DIR | Out-Null
    # Migrate once before Go starts independent financial test packages in parallel.
    $previousAdminURL = $env:PESAR_LEDGER_ADMIN_URL
    try {
        $env:PESAR_LEDGER_ADMIN_URL = $env:PESARO_LEDGER_TEST_ADMIN_URL
        & go run ./services/ledger/cmd/ledger-admin -synthetic -action=migrate | Tee-Object -FilePath (Join-Path $env:PESAR_LEDGER_EVIDENCE_DIR 'migrate.log')
        if ($LASTEXITCODE -ne 0) { throw 'Ledger evidence schema preparation failed.' }
    } finally { $env:PESAR_LEDGER_ADMIN_URL = $previousAdminURL }
    $previousReconciliationAdmin = $env:PESAR_RECONCILIATION_ADMIN_URL
    try {
        $env:PESAR_RECONCILIATION_ADMIN_URL = $env:PESAR_RECONCILIATION_TEST_ADMIN_URL
        & go run ./services/reconciliation/cmd/reconciliation-admin -synthetic | Tee-Object -FilePath (Join-Path $env:PESAR_LEDGER_EVIDENCE_DIR 'inbox-migrate.log')
        if ($LASTEXITCODE -ne 0) { throw 'Reconciliation evidence schema preparation failed.' }
    } finally { $env:PESAR_RECONCILIATION_ADMIN_URL = $previousReconciliationAdmin }
    $testArgs = @('test', '-count=1', '-v')
    if ($Race) { $testArgs += '-race' }
    $testArgs += @('./services/ledger/...', './services/reconciliation/...', './tests/eventdelivery')
    & go @testArgs | Tee-Object -FilePath (Join-Path $env:PESAR_LEDGER_EVIDENCE_DIR 'tests.log')
    if ($LASTEXITCODE -ne 0) { throw 'Ledger evidence tests failed.' }
    if ($RestartDatabase) {
        $artifact = Join-Path $env:PESAR_LEDGER_EVIDENCE_DIR 'restart-instruction.json'
        $env:PESAR_LEDGER_RESTART_PREPARE = $artifact
        try {
            & go test -count=1 -run '^Test(Database|Inbox)RestartPrepare$' -v ./services/ledger/internal/app ./services/reconciliation/internal/inbox | Tee-Object -FilePath (Join-Path $env:PESAR_LEDGER_EVIDENCE_DIR 'restart-prepare.log')
            if ($LASTEXITCODE -ne 0) { throw 'Database restart preparation failed.' }
        } finally { Remove-Item Env:PESAR_LEDGER_RESTART_PREPARE -ErrorAction SilentlyContinue }
        $drive = $ledgerRoot.Substring(0, 1).ToLowerInvariant()
        $linuxRoot = '/mnt/' + $drive + $ledgerRoot.Substring(2).Replace('\', '/')
        & wsl.exe -d Ubuntu -- bash "$linuxRoot/scripts/ledger/local-db.sh" crash
        if ($LASTEXITCODE -ne 0) { throw 'Verified local database crash failed.' }
        & wsl.exe -d Ubuntu -- bash "$linuxRoot/scripts/ledger/local-db.sh" start
        if ($LASTEXITCODE -ne 0) { throw 'Local database restart failed; run local-db.sh start to recover.' }
        $env:PESAR_LEDGER_RESTART_RECOVER = $artifact
        try {
            & go test -count=1 -run '^Test(Database|Inbox)RestartRecover$' -v ./services/ledger/internal/app ./services/reconciliation/internal/inbox | Tee-Object -FilePath (Join-Path $env:PESAR_LEDGER_EVIDENCE_DIR 'restart-recover.log')
            if ($LASTEXITCODE -ne 0) { throw 'Database restart recovery failed.' }
        } finally { Remove-Item Env:PESAR_LEDGER_RESTART_RECOVER -ErrorAction SilentlyContinue }
    }
    $record = [ordered]@{
        recorded_at = [DateTime]::UtcNow.ToString('o')
        commit = (& git rev-parse HEAD)
        worktree_dirty = [bool](& git status --porcelain)
        go_version = (& go version)
        engine = 'CockroachDB v26.2.3'
        topology = 'single synthetic local node; TLS; 16 adapter connections'
        database_restart = [bool]$RestartDatabase
        race_detection = [bool]$Race
        result = 'Selected Ledger tests passed; complete M1 and production gates remain separate.'
    }
    $record | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $env:PESAR_LEDGER_EVIDENCE_DIR 'run.json') -Encoding UTF8
    Write-Output 'Evidence saved under .cache/ledger-evidence. No production-readiness claim.'
} finally { Pop-Location }
