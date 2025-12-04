import boto3
import pyarrow as pa
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,ArrayType
from pyspark.sql.functions import input_file_name, explode, col, date_format, to_timestamp
import duckdb
''' code dùng cho fact table'''
S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS = "minio"
S3_SECRET = "minio123"
S3_BUCKET = "trading-okx"
#
def get_latest_file(bucket_name, prefix, file_ext,days_lookback):
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS,
        aws_secret_access_key=S3_SECRET,
        region_name="us-east-1"
    )
    all_objects = []
    # Paginator is used to list if more 1000 files
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    for page in page_iterator:
        for obj in page.get('Contents', []):
            key=obj['Key']
            last_modified = obj['LastModified']
            if last_modified > datetime.now(timezone.utc)- timedelta(days=days_lookback):
                 all_objects.append(obj)
    all_objects.sort(key=lambda x: x['LastModified'], reverse=True)
    #latest_files = all_objects[:limit]
    latest_files = all_objects
    paths = [f"s3a://{bucket_name}/{obj['Key']}" for obj in latest_files]

    return paths

# clean and load data into dim fact then load into duckdb
def process(latest_files):
    # spark connect
    # should change config in .sh
    spark = SparkSession.builder \
        .appName("OKX_Bronze_To_Silver_mark") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    mark_schema = StructType([
        StructField("instId", StringType(), True),
        StructField("instType", StringType(), True),
        StructField("markPx", StringType(), True),  # Giá Mark (String -> cần cast Double)
        StructField("ts", StringType(), True)  # Timestamp (String ms -> cần cast Timestamp)
    ])
    schema = StructType([
        StructField("received_at", StringType(), True),
        StructField("payload", StructType([
            StructField("data", ArrayType(mark_schema), True)
        ]), True)
    ])
    df = spark.read.schema(schema).json(latest_files)
    df.show(truncate=False)
    df_exploded = df.select(
        col("received_at"),
        explode(col("payload.data")).alias("mark")
    )
    df_exploded.show(truncate=False)
    # B3.2: Ép kiểu và Chọn cột
    df_silver = df_exploded.select(
        col("mark.instId").alias("symbol"),
        col("mark.instType").alias("instrument_type"),

        # 'markPx' là giá quan trọng nhất -> Cast về Double
        col("mark.markPx").cast("double").alias("mark_price"),

        # Xử lý thời gian sự kiện (Event Time)
        # 'ts' là milliseconds (13 chữ số) -> chia 1000 -> seconds
        to_timestamp(col("mark.ts").cast("long") / 1000).alias("event_time"),

        # Thời gian hệ thống nhận được (Ingestion Time)
        col("received_at").cast("timestamp").alias("ingestion_time")
    )
    #df_silver.show(truncate=False)
    df_cleaned = df_silver \
        .dropna(subset=["mark_price", "event_time"]) \
        .withColumn("date_part", date_format(col("event_time"), "yyyy-MM-dd"))

    duck_path = '/mnt/d/learn/DE/Semina_project/datawarehouse.duckdb'
    con = duckdb.connect(duck_path)
    # read file sql to connect to minio and create dim fact table

    # đoạn code dưới được đưa vào khi select from read_parquet(path), khi đọc file parquet trực tiếp vào duckdb thì
    # run code nó sẽ set up các config cho minio để kết nối trực tiếp đến duckdb -> duckdb có thể read direct data in minio
    # nếu ko có ko cần thêm vào
    '''with open('/mnt/d/learn/DE/Semina_project/SQL_db/config_dw/warehouse_source.sql', 'r') as f:
         sql_script = f.read()
    con.execute(sql_script)'''

    # dki dataframe thành bảng ảo arrow_table_virtual để query sql
    # convert pyspark dataframe to arrow table
    arrow_table = pa.Table.from_pandas(df_cleaned.toPandas())
    con.register("arrow_table_virtual", arrow_table)
    con.execute('''
                INSERT INTO fact_mark_price(
                        symbol ,
                        instrument_type ,
                        mark_price ,
                        event_time ,
                        ingestion_time,
                        date_part
                        )
                SELECT                         
                        symbol ,
                        instrument_type ,
                        mark_price ,
                        event_time ,
                        ingestion_time,
                        date_part
                FROM arrow_table
                ''')
    con.close()
    #format data and load into minio with parquet
    output = f"s3a://trading-okx/silver/okx-mark-price/"
    df_silver.write.mode('append').format("parquet").save(output)
    spark.stop()
def func_process():
    latest_files = get_latest_file(
        bucket_name="trading-okx",
        prefix="bronze/okx_mark_price/",
        file_ext=".jsonl.gz",
        days_lookback=6
    )
    process(latest_files)
func_process()