import json
from kafka import KafkaConsumer
import time
from cassandra.cluster import Cluster
cluster = Cluster(['0.0.0.0'],port=9042)
session = cluster.connect('products')
time1 = time.time()
consumer = KafkaConsumer('product1', bootstrap_servers=['localhost:9092'])
for message in consumer:
    # print ("Topic : {}, Partition : {}, Offset : {}, key : {}, value : {}".format(message.topic, message.partition, message.offset, message.key, message.value.decode('utf-8')))
    decoded_message = json.loads(message.value.decode('utf-8'))
    query_string = f"INSERT INTO products (PogId , Brand , Category, description , price, subcategory) VALUES ('{str(decoded_message['PogId'])}','{str(decoded_message['Brand'])}','{str(decoded_message['Category'])}','{str(decoded_message['Description'])}',{int(decoded_message['Price'])},'{str(decoded_message['SubCategory'])}')"
    record = session.execute_async(query_string)
    print('Record Consume Rate (Records/Second): ',consumer.metrics()['consumer-fetch-manager-metrics']['records-consumed-rate'])
time2 = time.time()
timedeltaa = time2 - time1
print(f'Total time taken for {i} Records: {timedeltaa*1000} ms')





# decoded_message = json.loads(message.value.decode('utf-8'))
# query_string = f"INSERT INTO products (PogId , Brand , Category , country , creationtime , description , price , quantity , sellercode , size , stock , subcategory , supc) VALUES ('{str(decoded_message['PogId'])}','{str(decoded_message['Brand'])}','{str(decoded_message['Category'])}','{str(decoded_message['Country'])}','{str(decoded_message['creationtime'])}','{str(decoded_message['Description'])}',{int(decoded_message['Price'])},{int(decoded_message['Quantity'])},'{str(decoded_message['SellerCode'])}','{str(decoded_message['Size'])}','{str(decoded_message['stock'])}','{str(decoded_message['SubCategory'])}','{str(decoded_message['Supc'])}')"
# record = session.execute_async(query_string)