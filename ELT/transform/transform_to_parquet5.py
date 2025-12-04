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
        .appName("OKX_Bronze_To_Silver_ohlc") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    candle_array_schema = ArrayType(StringType())
    schema = StructType([
        StructField("received_at", StringType(), True),
        StructField("payload", StructType([
            # Lấy thêm thông tin channel để biết instId (Symbol)
            StructField("arg", StructType([
                StructField("instId", StringType(), True),
                StructField("channel", StringType(), True)
            ]), True),
            # Data là mảng của các mảng
            StructField("data", ArrayType(candle_array_schema), True)
        ]), True)
    ])

    df = spark.read.schema(schema).json(latest_files)

    #df.show(truncate=False)
    df_exploded = df.select(
        col("received_at"),
        col("payload.arg.instId").alias("symbol"),
        col("payload.arg.channel").alias("channel"),
        explode(col("payload.data")).alias("candle")
    )
    #df_exploded.show(truncate=False)
    df_silver = df_exploded.select(
        col("symbol"),
        col("channel"),
        to_timestamp(col("candle")[0].cast("long") / 1000).alias("candle_time"),
        col("candle")[1].cast("double").alias("open"),
        col("candle")[2].cast("double").alias("high"),
        col("candle")[3].cast("double").alias("low"),
        col("candle")[4].cast("double").alias("close"),
        col("candle")[5].cast("double").alias("volume"),
        col("candle")[8].cast("int").alias("is_confirmed"),
        col("received_at").cast("timestamp").alias("ingestion_time")
    )
    #df_silver.show(truncate=False)
    df_cleaned = df_silver \
        .dropna(subset=["open", "candle_time"]) \
        .withColumn("date_part", date_format(col("candle_time"), "yyyy-MM-dd"))

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
                INSERT INTO fact_ohlc(
                                            symbol,
                                            channel,
                                            candle_time,
                                            open,
                                            high,
                                            low,
                                            close,
                                            volume,
                                            is_confirmed,
                                            ingestion_time,
                                            date_part)
                SELECT                         
                    symbol ,
                    channel ,
                    candle_time ,
                    open ,
                    high ,
                    low ,
                    close ,
                    volume , 
                    is_confirmed ,
                    ingestion_time ,
                    date_part 
                FROM arrow_table_virtual
                ''')
    con.close()
    #format data and load into minio with parquet
    output = f"s3a://trading-okx/silver/okx-ohlc/"
    df_silver.write.mode('append').format("parquet").save(output)
    spark.stop()
def func_process():
    latest_files = get_latest_file(
        bucket_name="trading-okx",
        prefix="bronze/okx_ohlc/",
        file_ext=".jsonl.gz",
        days_lookback=6
    )
    process(latest_files)
func_process()