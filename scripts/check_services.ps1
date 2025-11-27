# Script kiem tra nhanh cac services
# Chay: .\scripts\check_services.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "KIEM TRA CAC SERVICES" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Kiem tra Docker dang chay
Write-Host "1. Kiem tra Docker..." -ForegroundColor Yellow
try {
    $null = docker ps 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   OK Docker dang chay" -ForegroundColor Green
    }
    else {
        Write-Host "   FAIL Docker khong chay" -ForegroundColor Red
        exit 1
    }
}
catch {
    Write-Host "   FAIL Docker khong chay hoac co loi" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Danh sach containers can kiem tra
$containers = @(
    @{Name = "postgres"; Port = 5432 },
    @{Name = "kafka"; Port = 9092 },
    @{Name = "minio"; Port = 9000 },
    @{Name = "airflow"; Port = 8085 },
    @{Name = "spark-master"; Port = 7077 },
    @{Name = "spark-worker"; Port = 8084 }
)

Write-Host "2. Kiem tra containers..." -ForegroundColor Yellow
$allRunning = $true
foreach ($container in $containers) {
    $status = docker ps --filter "name=$($container.Name)" --format "{{.Status}}" 2>$null
    if ($status -like "*Up*") {
        Write-Host "   OK $($container.Name) dang chay" -ForegroundColor Green
    }
    else {
        Write-Host "   FAIL $($container.Name) khong chay" -ForegroundColor Red
        $allRunning = $false
    }
}
Write-Host ""

# Kiem tra ket noi cac services
Write-Host "3. Kiem tra ket noi services..." -ForegroundColor Yellow

# Test Airflow
try {
    $null = Invoke-WebRequest -Uri "http://localhost:8085/health" -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
    Write-Host "   OK Airflow UI: http://localhost:8085" -ForegroundColor Green
}
catch {
    Write-Host "   FAIL Airflow UI khong truy cap duoc" -ForegroundColor Red
}

# Test Spark Master
try {
    $null = Invoke-WebRequest -Uri "http://localhost:8082" -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
    Write-Host "   OK Spark Master UI: http://localhost:8082" -ForegroundColor Green
}
catch {
    Write-Host "   FAIL Spark Master UI khong truy cap duoc" -ForegroundColor Red
}

# Test MinIO
try {
    $null = Invoke-WebRequest -Uri "http://localhost:9001" -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
    Write-Host "   OK MinIO Console: http://localhost:9001" -ForegroundColor Green
}
catch {
    Write-Host "   FAIL MinIO Console khong truy cap duoc" -ForegroundColor Red
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
if ($allRunning) {
    Write-Host "TAT CA SERVICES HOAT DONG BINH THUONG!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Buoc tiep theo:" -ForegroundColor Yellow
    Write-Host "1. Tao Spark connection trong Airflow" -ForegroundColor White
    Write-Host "   docker exec -it airflow bash" -ForegroundColor Gray
    Write-Host "   airflow connections add spark_default --conn-type spark --conn-host spark-master --conn-port 7077" -ForegroundColor Gray
    Write-Host ""
    Write-Host "2. Truy cap Airflow UI: http://localhost:8085" -ForegroundColor White
    Write-Host "   Username: admin" -ForegroundColor Gray
    Write-Host "   Password: admin" -ForegroundColor Gray
}
else {
    Write-Host "CO MOT SO SERVICES CHUA CHAY!" -ForegroundColor Red
    Write-Host "Hay chay: docker-compose up -d --build" -ForegroundColor Yellow
}
Write-Host "========================================" -ForegroundColor Cyan
