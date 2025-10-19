# Taxi Big Data Platform - Setup Script
# This script helps you set up and test the CDC pipeline

Write-Host "===============================================" -ForegroundColor Cyan
Write-Host "  Taxi Big Data Platform - CDC Setup" -ForegroundColor Cyan
Write-Host "===============================================" -ForegroundColor Cyan
Write-Host ""

# Function to check if Docker is running
function Test-DockerRunning {
    try {
        docker info > $null 2>&1
        return $true
    }
    catch {
        return $false
    }
}

# Check Docker
Write-Host "[1/6] Checking Docker..." -ForegroundColor Yellow
if (Test-DockerRunning) {
    Write-Host "[OK] Docker is running" -ForegroundColor Green
}
else {
    Write-Host "[ERROR] Docker is not running" -ForegroundColor Red
    Write-Host "Please start Docker Desktop and try again" -ForegroundColor Red
    exit 1
}

# Check Python
Write-Host "`n[2/6] Checking Python..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "[OK] Python found: $pythonVersion" -ForegroundColor Green
}
catch {
    Write-Host "[ERROR] Python not found" -ForegroundColor Red
    Write-Host "Please install Python 3.8+ and try again" -ForegroundColor Red
    exit 1
}

# Install Python dependencies
Write-Host "`n[3/6] Installing Python dependencies..." -ForegroundColor Yellow
pip install -r requirements.txt --quiet
if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Python dependencies installed" -ForegroundColor Green
}
else {
    Write-Host "[WARNING] Some dependencies may not have installed correctly" -ForegroundColor Yellow
}

# Start Docker Compose services
Write-Host "`n[4/6] Starting Docker services..." -ForegroundColor Yellow
Write-Host "This may take a few minutes for first-time setup..." -ForegroundColor Cyan
docker-compose up -d

if ($LASTEXITCODE -eq 0) {
    Write-Host "[OK] Docker services started" -ForegroundColor Green
}
else {
    Write-Host "[ERROR] Failed to start Docker services" -ForegroundColor Red
    exit 1
}

# Wait for services to be ready
Write-Host "`n[5/6] Waiting for services to be ready..." -ForegroundColor Yellow
Write-Host "Waiting 45 seconds for services to initialize..." -ForegroundColor Cyan

$services = @(
    @{Name="PostgreSQL"; Port=5432},
    @{Name="Kafka"; Port=9092},
    @{Name="Debezium"; Port=8083},
    @{Name="Kafka UI"; Port=8080}
)

Start-Sleep -Seconds 45

foreach ($service in $services) {
    $connection = Test-NetConnection -ComputerName localhost -Port $service.Port -WarningAction SilentlyContinue
    if ($connection.TcpTestSucceeded) {
        Write-Host "[OK] $($service.Name) is ready (port $($service.Port))" -ForegroundColor Green
    }
    else {
        Write-Host "[WARNING] $($service.Name) may not be ready yet (port $($service.Port))" -ForegroundColor Yellow
    }
}

# Setup Debezium connector
Write-Host "`n[6/6] Setting up Debezium connector..." -ForegroundColor Yellow
Write-Host "This will register the CDC connector with Kafka Connect..." -ForegroundColor Cyan

python scripts/setup_debezium.py

Write-Host "`n===============================================" -ForegroundColor Cyan
Write-Host "  Setup Complete!" -ForegroundColor Cyan
Write-Host "===============================================" -ForegroundColor Cyan

Write-Host "`nNext steps:" -ForegroundColor Yellow
Write-Host "1. Run tests: python scripts/test_cdc.py" -ForegroundColor White
Write-Host "2. Open Kafka UI: http://localhost:8080" -ForegroundColor White
Write-Host "3. Check logs: docker-compose logs -f" -ForegroundColor White

Write-Host "`nUseful commands:" -ForegroundColor Yellow
Write-Host "- Stop services and remove data:  docker-compose down -v" -ForegroundColor White
Write-Host "- View logs:      docker-compose logs -f [service-name]" -ForegroundColor White
Write-Host "- Check status:   docker-compose ps" -ForegroundColor White

Write-Host ""
