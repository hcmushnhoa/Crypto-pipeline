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
        .appName("OKX_Bronze_To_Silver_book") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    fund_schema = StructType([
        StructField("formulaType", StringType(), True),
        StructField("fundingRate", StringType(), True),
        StructField("fundingTime", StringType(), True),
        StructField("impactValue", StringType(), True),
        StructField("instId", StringType(), True),
        StructField("instType", StringType(), True),
        StructField("interestRate", StringType(), True),
        StructField("maxFundingRate", StringType(), True),
        StructField("method", StringType(), True),
        StructField("minFundingRate", StringType(), True),
        StructField("nextFundingRate", StringType(), True),
        StructField("nextFundingTime", StringType(), True),
        StructField("premium", StringType(), True),
        StructField("settFundingRate", StringType(), True),
        StructField("settState", StringType(), True),
        StructField("ts", StringType(), True)
    ])
    schema = StructType([
        StructField("received_at", StringType(), True),
        StructField("payload", StructType([
            StructField("data", ArrayType(fund_schema), True)
        ]), True)
    ])
    df = spark.read.schema(schema).json(latest_files)
    df.show(truncate=False)
    df_exploded = df.select(
        col("received_at"),
        explode(col("payload.data")).alias("fund")
    )
    #df_exploded.show(truncate=False)
    # B3.2: Ép kiểu và Chọn cột
    df_silver = df_exploded.select(
        col("fund.instId").alias("symbol"),
        col("fund.instType").alias("instrument_type"),

        # Funding Rate là số rất nhỏ, cần cast Double
        col("fund.fundingRate").cast("double").alias("funding_rate"),
        col("fund.nextFundingRate").cast("double").alias("next_funding_rate"),

        # Xử lý thời gian (Unix ms -> Timestamp)
        to_timestamp(col("fund.fundingTime").cast("long") / 1000).alias("funding_time"),
        to_timestamp(col("fund.nextFundingTime").cast("long") / 1000).alias("next_funding_time"),

        # Thời gian nhận dữ liệu (để debug độ trễ)
        col("received_at").cast("timestamp").alias("ingestion_time")
    )
    df_silver.show(truncate=False)
    df_cleaned = df_silver \
        .drop(col("next_funding_rate")) \
        .withColumn("date_part", date_format(col("funding_time"), "yyyy-MM-dd"))
    #df_silver.show(truncate=False)

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
    arrow_table = pa.Table.from_pandas(df_silver.toPandas())
    con.register("arrow_table_virtual", arrow_table)
    con.execute('''
                INSERT INTO fact_funding_rate(
                    symbol,
                    instrument_type,
                    funding_rate,
                    next_funding_rate,
                    funding_time,
                    next_funding_time,
                    ingestion_time,
                    date_part
                )
                SELECT symbol,
                       instrument_type,
                       funding_rate,
                       next_funding_rate,
                       funding_time,
                       next_funding_time,
                       ingestion_time,
                        date_part
                FROM arrow_table
                ''')
    con.close()
    #format data and load into minio with parquet
    output = f"s3a://trading-okx/silver/okx-funding/"
    df_silver.write.mode('append').format("parquet").save(output)
    spark.stop()
def func_process():
    latest_files = get_latest_file(
        bucket_name="trading-okx",
        prefix="bronze/okx_funding/",
        file_ext=".jsonl.gz",
        days_lookback=6
    )
    process(latest_files)
func_process()