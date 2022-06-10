from kafka import KafkaProducer
import pandas as pd
import time
df = pd.read_csv('products.csv')
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],retries=5)

def on_send_success(record_metadata):
    print("Topic: ",record_metadata.topic,end=" ")
    print("Partition: ",record_metadata.partition,end=" ")
    print("Offset: ",record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)

time1 = time.time()
for index, row in df.iterrows():
    producer.send('product3', row.to_json().encode('utf-8')).add_callback(on_send_success).add_errback(on_send_error)
    print('Record Send Rate (Records/Second): ',producer.metrics()['producer-metrics']['record-send-rate'])
producer.send('product3', b'end').add_callback(on_send_success).add_errback(on_send_error)
time2 = time.time()
timedeltaa = time2 - time1
print(f'Total time taken for Records: {timedeltaa*1000} ms')
producer.flush()
