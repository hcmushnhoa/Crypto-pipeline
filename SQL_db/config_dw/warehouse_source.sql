INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='minio';
SET s3_secret_access_key='minio123';
SET s3_use_ssl=false;
SET s3_url_style='path';
-- create dim fact here
