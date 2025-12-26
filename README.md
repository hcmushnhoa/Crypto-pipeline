# Trading Pipeline from OKX

## Overview

---
Dự án là hệ thống data pipeline được xây dựng bằng Apache Airflow kết hợp với kiến trúc phân tán Spark để thu thập, xử lý và lưu trữ dữ liệu tiền điện tử Bitcoin từ sàn giao dịch OKX


## Project Structure

---
```text
.
├── Dockerfile
├── ELT
│   ├── __init__.py
│   ├── extract
│   │   └── script
│   │       ├── crawl_stream.py
│   │       └── create_topics.py
│   ├── load
│   │   └── load_to_minio.py
│   └── transform
│       ├── agg_orderbook.py
│       ├── agg_trades.py
│       ├── derives_features.py
│       ├── index_price_kline.py
│       ├── mark_price_kline.py
│       ├── merge_features.py
│       ├── trans_ohlc_cal.py
│       ├── trans_tables_db.py
│       └── transform_test.ipynb
├── README.md
├── SQL_db
│   ├── config_database
│   │   └── source_db.sql
│   └── config_dw
│       ├── droptable.py
│       ├── select.py
│       ├── set_up_duckdb.py
│       └── warehouse_source.sql
├── backend
│   └── extract
│       └── script
│           ├── craw_rest_ohlc.py
│           ├── crawl_rest_ohlc.ipynb
│           └── crawl_rest_open_interest.py
├── dags
│   ├── dag_02.py
│   └── dag_03.py
├── datawarehouse.duckdb
├── docker-compose.yml
├── plugins
│   ├── __init__.py
│   ├── fairscheduler.xml
│   └── spark_config.py
├── requirements.txt
└── schedule
```

## Features

---

- Data ingestion: Request data từ websocket của sàn OKX và chuyển trực tiếp lên Redpanda
- Distributed Data Storage: Sử dụng hệ thống lưu trữ phân tán tương thích với giao diện S3 của AWS(MinIO)
- Large-Scale Data Processing: Sử dụng Apache Spark để xử lý song song và chuyển đổi data một cách hiệu quả
- Structured and Storaged Data Warehousing: Sử dụng DuckDB làm động cơ để thực hiện các truy vấn tức thì phục vụ cho việc phân tích
- Containerized Deployment: Các thành phần (Apache Airflow, Spark, Redpanda, Minio, DuckDB) được đóng gói bằng Docker và Docker Compose dễ dàng mang đi và sử dụng
- Automated Workflows: Khả năng tự động khởi tạo hệ thống thu thập, xử lý và lưu trữ data

---

## Prerequisites

---

- Docker và Docker Compose
- Python 3.8+
- Airflow 2.10.5


## Technical Architecture

---

Sử dụng quy trình Extract-Load-Transform(ELT) để xây dựng pipeline theo kiến trúc MEDALLION để cải thiện chất lượng và cấu trúc của data
- Data collection: Các đoạn code Python (ELT/extract/script/crawl_stream.py và backend/extract/script/crawl.py)
tương tác với API và Webserver của OKX để truy xuất data của sàn giao dịch, dữ liệu thu thập được lưu dưới dạng JSON
- Data staging: Các file JSON(raw) được load vào Minio để lưu trữ trong Bronze layer(lớp đầu của kiến trúc MEDALLION) bằng file code Python(ELT/load/load_to_minio.py)
- Data processing:
  - Cấu hình cho spark để nó có thể kết nối với MinIO(plugins/spark_config.py)
  - Chuyển đổi data từ dạng raw sang parquet phục vụ cho tổng hợp data(ETL/transform/trans_table_db.py)
  - Tổng hợp data để chuẩn bị cho phân tích(ETL/transform/agg_trades, TL/transform/agg_orderbook) 
- Data service:
  - Data được tổng hợp lưu vào gold layer để phục vụ phân tích và build Models
- Orchestration & Management: 
  - Sử dụng Apache Airflow để tạo vòng đời cho hệ thống 
  - Các tập lệnh python và shell sử dụng để xây dựng hệ thống
  - Docker Compose được sử dụng đẻ quản lý toàn bộ hệ thống
- Visualization



## Usage

---

1. Truy cập `localhost:9001` để kiểm tra không gian lưu trữ của minio:
- Thông tin đăng nhập của Minio:
   - username: minio
   - password: minio123
2. Truy cập `localhost:8888` để sử dụng web UI của Airflow:
- Thông tin đăng nhập của Airflow: 
   - username: admin
   - password: admin 
- Truy cập vào DAGs để giám sát và kích hoạt pipeline
