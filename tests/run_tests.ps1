# PowerShell script to run Selenium tests for DB Migrator
# Usage: .\run_tests.ps1 [-TestType <type>] [-Browser <browser>] [-Headful]

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("all", "smoke", "integration", "e2e", "fast")]
    [string]$TestType = "all",
    
    [Parameter(Mandatory=$false)]
    [ValidateSet("chrome", "firefox")]
    [string]$Browser = "chrome",
    
    [Parameter(Mandatory=$false)]
    [switch]$Headful = $false
)

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "DB Migrator - Selenium Test Runner" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""

# Check if application is running
Write-Host "Checking if application is running..." -ForegroundColor Yellow
$containerStatus = docker ps --filter name=db-migrator --format "{{.Status}}"

if (-not $containerStatus) {
    Write-Host "ERROR: db-migrator container is not running!" -ForegroundColor Red
    Write-Host "Please start the application first:" -ForegroundColor Yellow
    Write-Host "  docker-compose up -d" -ForegroundColor White
    exit 1
}

if ($containerStatus -like "*healthy*") {
    Write-Host "✓ Application is healthy" -ForegroundColor Green
} elseif ($containerStatus -like "*starting*") {
    Write-Host "Application is still starting..." -ForegroundColor Yellow
    Write-Host "Waiting for health check..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10
} else {
    Write-Host "WARNING: Application status: $containerStatus" -ForegroundColor Yellow
}

Write-Host ""

# Check if dependencies are installed
Write-Host "Checking dependencies..." -ForegroundColor Yellow

try {
    python -c "import selenium" 2>$null
    Write-Host "✓ Selenium is installed" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Selenium is not installed!" -ForegroundColor Red
    Write-Host "Install with: pip install selenium" -ForegroundColor Yellow
    exit 1
}

try {
    python -c "import pytest" 2>$null
    Write-Host "✓ Pytest is installed" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Pytest is not installed!" -ForegroundColor Red
    Write-Host "Install with: pip install pytest pytest-timeout" -ForegroundColor Yellow
    exit 1
}

Write-Host ""

# Build pytest command
$pytestArgs = @("tests/test_selenium.py", "-v", "--browser=$Browser")

switch ($TestType) {
    "smoke" {
        $pytestArgs += "-m", "smoke"
        Write-Host "Running SMOKE tests only" -ForegroundColor Cyan
    }
    "integration" {
        $pytestArgs += "-m", "integration"
        Write-Host "Running INTEGRATION tests only" -ForegroundColor Cyan
    }
    "e2e" {
        $pytestArgs += "-m", "e2e"
        Write-Host "Running END-TO-END tests only" -ForegroundColor Cyan
    }
    "fast" {
        $pytestArgs += "-m", "not slow"
        Write-Host "Running FAST tests (excluding slow)" -ForegroundColor Cyan
    }
    default {
        Write-Host "Running ALL tests" -ForegroundColor Cyan
    }
}

Write-Host "Browser: $Browser" -ForegroundColor Cyan

if ($Headful) {
    Write-Host "Mode: HEADFUL (browser window visible)" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "NOTE: You need to manually edit test_selenium.py to disable headless mode" -ForegroundColor Yellow
    Write-Host "Comment out lines 151 in test_selenium.py:" -ForegroundColor Yellow
    Write-Host "  # options.add_argument('--headless')" -ForegroundColor White
    Write-Host ""
    $continue = Read-Host "Have you disabled headless mode? (y/n)"
    if ($continue -ne "y") {
        Write-Host "Aborting..." -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "Mode: HEADLESS (no browser window)" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Starting tests..." -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""

# Run pytest
$startTime = Get-Date
& pytest @pytestArgs
$exitCode = $LASTEXITCODE
$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host ""
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Test Run Complete" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Duration: $($duration.ToString('mm\:ss'))" -ForegroundColor Cyan
Write-Host "Exit Code: $exitCode" -ForegroundColor $(if ($exitCode -eq 0) { "Green" } else { "Red" })
Write-Host ""

if ($exitCode -eq 0) {
    Write-Host "✓ All tests passed!" -ForegroundColor Green
} else {
    Write-Host "✗ Some tests failed. See output above for details." -ForegroundColor Red
}

exit $exitCode
