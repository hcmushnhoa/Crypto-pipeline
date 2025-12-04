# create_topics.py
from confluent_kafka.admin import AdminClient, NewTopic
c = AdminClient({"bootstrap.servers": "localhost:8080"})
new_topics = [NewTopic("okx_trades", num_partitions=6, replication_factor=1),
              NewTopic("okx_orderbook", num_partitions=6, replication_factor=1),
              NewTopic("okx_ohlc", num_partitions=6, replication_factor=1),
              NewTopic("okx_mark_price", num_partitions=1, replication_factor=1),
              NewTopic("okx_funding", num_partitions=1, replication_factor=1)]
fs = c.create_topics(new_topics)
for topic, f in fs.items():
    try:
        f.result()
        print("Created", topic)
    except Exception as e:
        print("Failed creating", topic, e)
