# Setup Guide - Taxi BigData Platform

## Yêu cầu hệ thống

- **Docker Desktop** (hoặc Docker Engine + Docker Compose)
- **Git**
- **Java 17** (nếu chạy Spark jobs local)
- **Python 3.10+** (tùy chọn, cho local development)

## Bước 1: Clone repository

```bash
git clone https://github.com/daoth0061/Taxi-BigData-Platform.git
cd Taxi-BigData-Platform
```

## Bước 2: Build Airflow image

**⚠️ QUAN TRỌNG**: Phải build image trước khi chạy `docker-compose up`

```bash
docker-compose build airflow
```

Hoặc build tất cả services:

```bash
docker-compose build
```

## Bước 3: Khởi động tất cả services

```bash
docker-compose up -d
```

Lệnh này sẽ khởi động:
- PostgreSQL (port 5432) - Database chính
- Kafka (port 9092) - Message broker
- Kafka UI (port 8080) - Kafka monitoring
- Debezium (port 8083) - CDC connector
- MinIO (port 9000, 9001) - S3-compatible storage
- Airflow (port 8085) - Workflow orchestration
- Spark Master (port 7077, 8082) - Distributed computing
- Spark Worker (port 8084)

## Bước 4: Kiểm tra services

```bash
docker-compose ps
```

Tất cả containers phải ở trạng thái `Up` hoặc `healthy`.

### Truy cập Web UIs:

- **Airflow**: http://localhost:8085 (admin/admin)
- **Kafka UI**: http://localhost:8080
- **MinIO Console**: http://localhost:9001 (admin/12345678)
- **Spark Master**: http://localhost:8082

## Bước 5: Tạo Spark connection trong Airflow

```bash
docker exec -it airflow bash
airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark-master \
  --conn-port 7077
exit
```

## Bước 6: Chạy data generation (tùy chọn)

Nếu muốn test với data:

```bash
# Cài dependencies local (tùy chọn)
pip install -r requirements.txt

# Chạy data generator
python src/data_generation/insert_taxi_data.py
```

## Bước 7: Trigger Airflow DAG

Truy cập http://localhost:8085, login với `admin/admin`, và trigger DAG `spark_silver_job`.

## Dừng services

```bash
# Dừng containers nhưng giữ data
docker-compose down

# Dừng và xóa tất cả data
docker-compose down -v
```

## Xử lý sự cố

### Build fails với lỗi "cannot access entrypoint.sh"

Đảm bảo bạn đang ở branch `feat/airflow-spark-scheduler` với code mới nhất:

```bash
git checkout feat/airflow-spark-scheduler
git pull
docker-compose build --no-cache airflow
```

### Airflow không start

Kiểm tra logs:

```bash
docker logs airflow
```

Nếu bị lỗi database, xóa và tạo lại:

```bash
docker-compose down
rm -rf airflow/db_data
docker-compose up -d
```

### Port conflicts

Nếu ports đã bị chiếm dụng, sửa trong `docker-compose.yml`:

```yaml
ports:
  - "8086:8080"  # Đổi 8085 thành port khác
```

## Cấu trúc thư mục quan trọng

```
├── src/
│   ├── airflow/
│   │   ├── dags/              # Airflow DAGs
│   │   ├── Dockerfile.airflow # Airflow image definition
│   │   └── airflow-requirements.txt
│   ├── streaming/             # Kafka to Iceberg jobs
│   ├── silver.py             # Silver layer transformation
│   └── gold.py               # Gold layer transformation
├── configs/                   # Configuration files
├── spark/                     # Spark entrypoint scripts
└── docker-compose.yml        # Services orchestration
```

## Lưu ý

- **Lần đầu chạy**: Container airflow sẽ mất ~2-3 phút để init database
- **Data persistence**: Data được lưu trong volumes `db_data`, `kafka_data`, và `./minio_data`
- **Logs**: Airflow logs nằm trong `./airflow/logs`
