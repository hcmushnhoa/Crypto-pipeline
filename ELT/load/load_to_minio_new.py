import json
import gzip
import threading
import time
from datetime import datetime
from confluent_kafka import Consumer
import boto3
import logging

# --- CONFIG LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- CONFIG APP ---
KAFKA_BOOTSTRAP = "localhost:9092"
TOPICS = ["okx_trades", "okx_funding", "okx_mark_price", "okx_ohlc", "okx_orderbook"]

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS = "minio"
S3_SECRET = "minio123"
S3_BUCKET = "trading-okx"  # Bucket mới của bạn

FLUSH_INTERVAL = 10
MAX_BUFFER_BYTES = 1 * 1024  # 200KB (Cấu hình thực tế hợp lý hơn 1KB)

# --- SETUP S3 & KAFKA ---
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS,
    aws_secret_access_key=S3_SECRET,
    region_name="us-east-1"
)

c = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "trade-bronze-consumer",  # Đổi group ID mới để đọc lại từ đầu cho chắc
    "auto.offset.reset": "earliest"
})
c.subscribe(TOPICS)

buffers = {}
buf_lock = threading.Lock()
stop_event = threading.Event()


# --- FUNCTIONS ---

def s3_put_bytes(file_key, data_bytes):
    """Upload data lên MinIO"""
    try:
        try:
            s3.head_bucket(Bucket=S3_BUCKET)
        except Exception:
            logger.warning(f"Bucket {S3_BUCKET} not found, creating...")
            s3.create_bucket(Bucket=S3_BUCKET)

        # Upload
        s3.put_object(Bucket=S3_BUCKET, Key=file_key, Body=data_bytes)
        logger.info(f"Uploaded: {file_key} ({len(data_bytes)} bytes)")
        return True
    except Exception as e:
        logger.error(f"Upload ERROR: {e}")
        return False


def flush_key(k):
    """Lấy data từ buffer, nén Gzip và upload (Dùng cho Scheduled Flush)"""
    with buf_lock:
        if k not in buffers or len(buffers[k]) == 0:
            return

        # Lấy data và xóa buffer ngay lập tức
        data = bytes(buffers[k])
        buffers[k] = bytearray()

    try:
        topic, dh = k.split("|")
        date, hour = dh.rsplit("-", 1)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        object_key = f"bronze/{topic}/{date}/{hour}/{topic}_{ts}.jsonl.gz"

        # --- FIX QUAN TRỌNG: NÉN GZIP TRƯỚC KHI UPLOAD ---
        compressed_data = gzip.compress(data)

        s3_put_bytes(object_key, compressed_data)

    except Exception as e:
        logger.error(f"Error flushing key {k}: {e}")


def flush_thread_func():
    """Thread chạy ngầm định kỳ"""
    logger.info("⏳ Flush thread started")
    while not stop_event.is_set():
        if stop_event.wait(FLUSH_INTERVAL):
            break

        with buf_lock:
            keys = list(buffers.keys())

        for k in keys:
            if len(buffers[k]) > 0:
                flush_key(k)
    logger.info("⏳ Flush thread stopped.")


def flush_all_remaining():
    logger.info("💾 Flushing ALL remaining data...")
    with buf_lock:
        keys = list(buffers.keys())

    count = 0
    for k in keys:
        if len(buffers[k]) > 0:
            flush_key(k)
            count += 1
    logger.info(f"🏁 Final flush completed. Processed {count} keys.")


def add_record(topic, record):
    ts = record.get("received_at")
    try:
        dt = datetime.fromisoformat(ts) if ts else datetime.now()
    except:
        dt = datetime.now()

    date = dt.strftime("%Y-%m-%d")
    hour = dt.strftime("%H")
    key = f"{topic}|{date}-{hour}"

    # Chuẩn bị dòng JSONL
    line = json.dumps(record) + "\n"

    # Lưu ý: Ta lưu text (bytes) vào buffer, chỉ nén khi upload
    line_bytes = line.encode('utf-8')

    with buf_lock:
        if key not in buffers:
            buffers[key] = bytearray()
        buffers[key].extend(line_bytes)

        current_len = len(buffers[key])

        # FLUSH NGAY LẬP TỨC NẾU ĐẦY
        if current_len >= MAX_BUFFER_BYTES:
            logger.info(f"⚡ Buffer full for {key} ({current_len} bytes), flushing...")

            # Copy data ra và clear buffer
            data_to_upload = bytes(buffers[key])
            buffers[key] = bytearray()

            # Tạo đường dẫn (Logic giống hệt flush_key)
            filepath = f"bronze/{topic}/{date}/{hour}/{topic}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl.gz"

            # Chạy thread riêng để upload (đã fix nén Gzip)
            threading.Thread(
                target=lambda: s3_put_bytes(
                    filepath,
                    gzip.compress(data_to_upload)  # <--- ĐÃ CÓ GZIP Ở ĐÂY (Code cũ của bạn đúng chỗ này)
                ),
                daemon=True
            ).start()


def main():
    t = threading.Thread(target=flush_thread_func)
    t.start()

    print(f"🚀 Consumer started. Listening on {KAFKA_BOOTSTRAP}")
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                val = json.loads(msg.value().decode())
                add_record(msg.topic(), val)
                # print(".", end="", flush=True) # Uncomment nếu muốn xem dot log
            except Exception as e:
                logger.error(f"Decode error: {e}")

    except KeyboardInterrupt:
        logger.info("\n🛑 User stopped consumer...")
    finally:
        stop_event.set()
        t.join()
        flush_all_remaining()
        c.close()
        logger.info("👋 Consumer stopped gracefully.")


if __name__ == "__main__":
    main()