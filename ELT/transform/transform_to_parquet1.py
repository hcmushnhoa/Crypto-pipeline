import boto3
import pyarrow as pa
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,ArrayType
from pyspark.sql.functions import input_file_name, explode, col, date_format
import duckdb
''' code dùng cho fact table'''
S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS = "minio"
S3_SECRET = "minio123"
S3_BUCKET = "trading-okx"
#
def get_latest_file(bucket_name, prefix,days_lookback):
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
        .appName("OKX_Bronze_To_Silver_trade") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    trade_schema = StructType([
            StructField("instId", StringType(), True),
            StructField("tradeId", StringType(), True),
            StructField("px", StringType(), True),
            StructField("sz", StringType(), True),
            StructField("side", StringType(), True),
            StructField("ts", StringType(), True),
            StructField("count", StringType(), True),
            StructField("source", StringType(), True),
            StructField("seqId", StringType(), True)
        ])
    schema = StructType([
            StructField("received_at", StringType(), True),
            StructField("payload", StructType([
                StructField("data", ArrayType(trade_schema), True)
            ]), True)
        ])

    df=spark.read.schema(schema).json(latest_files)

    #df.show(truncate=False)
    df_exploded = df.select(
            col("received_at"),
            explode(col("payload.data")).alias("trade")
        )
    df_silver = df_exploded.select(
            col("trade.instId").alias("symbol"),
            col("trade.tradeId").alias("tradeId"),
            col("trade.side").alias("side"),
            col("trade.px").cast("double").alias("price"),
            col("trade.sz").cast("double").alias("quantity"),
            (col("trade.ts").cast("long") / 1000).cast("timestamp").alias("trade_time"),
            col("received_at").cast("timestamp").alias("ingestion_time")
            )
    df_cleaned = df_silver \
        .dropna(subset=["price", "quantity"]) \
        .dropDuplicates(["tradeId"]) \
        .withColumn("date_part", date_format(col("trade_time"), "yyyy-MM-dd"))  # Cột dùng để partition
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
    arrow_table = pa.Table.from_pandas(df_cleaned.toPandas())
    con.register("arrow_table_virtual", arrow_table)
    con.execute('''
                INSERT INTO fact_trades(
                        symbol,
                        tradeId ,
                        side ,
                        price ,
                        quantity ,
                        trade_time ,
                        ingestion_time,
                        date_part
                )
                SELECT 
                        symbol,
                        tradeId ,
                        side ,
                        price ,
                        quantity ,
                        trade_time ,
                        ingestion_time,
                        date_part
                FROM arrow_table_virtual
    ''')
    con.close()
    #format data and load into minio with parquet
    output = f"s3a://{S3_BUCKET}/silver/okx_trades/"
    df_silver.write.mode('append').format("parquet").save(output)
    spark.stop()
def func_process():
    latest_files = get_latest_file(
        bucket_name=S3_BUCKET,
        prefix="bronze/okx_trades/",
        days_lookback=6
    )
    process(latest_files)
func_process()