param(
    [string]$PgDsn = "postgresql://postgres:postgres@localhost:5432/wage_cpi",
    [switch]$SkipInstall
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Push-Location $PSScriptRoot
try {
    Write-Host "Step 1/6: Install Python packages"
    if (-not $SkipInstall) {
        python -m pip install pandas requests psycopg
    } else {
        Write-Host "Skipped package install."
    }

    Write-Host "Step 2/6: Fetch raw CSV data"
    python .\src\00_fetch_to_raw_csv.py

    Write-Host "Step 3/6: Validate raw files"
    $rawFiles = @(
        ".\data\raw\wage_raw.csv",
        ".\data\raw\cpi_raw.csv"
    )
    $rawInfo = Get-Item $rawFiles | Select-Object Name, Length
    $rawInfo | Format-Table -AutoSize | Out-Host
    $emptyRawFiles = @($rawInfo | Where-Object { $_.Length -le 0 })
    if ($emptyRawFiles.Count -gt 0) {
        throw "One or more raw files are empty. Stop."
    }

    Write-Host "Step 4/6: Set PG_DSN"
    $env:PG_DSN = $PgDsn
    Write-Host "Using PG_DSN=$env:PG_DSN"

    Write-Host "Step 5/6: Clean + load into Postgres"
    python .\src\01_load_raw_to_postgres_clean.py

    Write-Host "Step 6/6: Export merged CSV from Postgres"
    python .\src\02_export_merged_for_jamovi_postgres.py

    Write-Host "Done. Output files:"
    Get-Item @(
        ".\data\processed\wage_clean.csv",
        ".\data\processed\cpi_clean.csv",
        ".\data\processed\wage_cpi_merged_from_sql.csv"
    ) | Select-Object Name, Length | Format-Table -AutoSize | Out-Host
}
finally {
    Pop-Location
}
