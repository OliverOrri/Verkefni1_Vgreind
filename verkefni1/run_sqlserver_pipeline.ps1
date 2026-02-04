param(
    [string]$SqlServerConnStr = "",
    [switch]$SkipInstall
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Push-Location $PSScriptRoot
try {
    Write-Host "Step 1/6: Install Python packages"
    if (-not $SkipInstall) {
        python -m pip install pandas requests pyodbc
    } else {
        Write-Host "Skipped package install."
    }

    Write-Host "Step 2/6: Fetch raw CSV data"
    python .\src\00_fetch_to_raw_csv.py
    if ($LASTEXITCODE -ne 0) { throw "Fetch step failed." }

    Write-Host "Step 3/6: Validate raw files"
    $rawInfo = Get-Item @(".\data\raw\wage_raw.csv", ".\data\raw\cpi_raw.csv") | Select-Object Name, Length
    $rawInfo | Format-Table -AutoSize | Out-Host
    $emptyRawFiles = @($rawInfo | Where-Object { $_.Length -le 0 })
    if ($emptyRawFiles.Count -gt 0) {
        throw "One or more raw files are empty. Stop."
    }

    Write-Host "Step 4/6: Set SQLSERVER_CONN_STR"
    if ($SqlServerConnStr -ne "") {
        $env:SQLSERVER_CONN_STR = $SqlServerConnStr
        Write-Host "SQLSERVER_CONN_STR is set for this session."
    } else {
        Write-Host "SQLSERVER_CONN_STR not set. Python script will auto-detect ODBC driver and use localhost defaults."
    }

    Write-Host "Step 5/6: Clean + load into SQL Server"
    python .\src\01_load_raw_to_sqlserver_clean.py
    if ($LASTEXITCODE -ne 0) { throw "SQL Server load step failed." }

    Write-Host "Step 6/6: Export merged CSV from SQL Server"
    python .\src\02_export_merged_for_jamovi_sqlserver.py
    if ($LASTEXITCODE -ne 0) { throw "Export step failed." }

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
