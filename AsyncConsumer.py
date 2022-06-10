import json
from kafka import KafkaConsumer
import time
from cassandra.cluster import Cluster
cluster = Cluster(['0.0.0.0'],port=9042)
session = cluster.connect('products')

import psycopg2
#Establishing the connection
conn = psycopg2.connect(database="products", user='postgres', password='root', host='127.0.0.1', port= '5432')
#Setting auto commit false
conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS PRODUCTS")
sql ='''CREATE TABLE PRODUCTS(
   POGID CHAR(30) NOT NULL,
   SUPC CHAR(20) PRIMARY KEY,
   PRICE INT,
   QUANTITY INT
)'''
cursor.execute(sql)
print("Table created successfully........")
conn.commit()
time1 = time.time()
consumer = KafkaConsumer('product3', bootstrap_servers=['localhost:9092'])
for message in consumer:
    # print ("Topic : {}, Partition : {}, Offset : {}, key : {}, value : {}".format(message.topic, message.partition, message.offset, message.key, message.value.decode('utf-8')))
    decoded_message = json.loads(message.value.decode('utf-8'))
    query_string = f'''INSERT INTO products (PogId , Brand , Category , country , description , sellercode , size , subcategory , supc) VALUES ('{str(decoded_message['PogId'])}','{str(decoded_message['Brand'])}','{str(decoded_message['Category'])}','{str(decoded_message['Country'])}','{str(decoded_message['Description'])}','{str(decoded_message['SellerCode'])}','{str(decoded_message['Size'])}','{str(decoded_message['SubCategory'])}','{str(decoded_message['Supc'])}')'''
    record = session.execute_async(query_string)
    # record = session.execute(query_string)

    cursor.execute(f'''INSERT INTO PRODUCTS(POGID, SUPC, PRICE, QUANTITY) VALUES ('{str(decoded_message['PogId'])}','{str(decoded_message['Supc'])}','{int(decoded_message['Price'])}','{int(decoded_message['Quantity'])}')''')

    print('Record Consume Rate (Records/Second): ',consumer.metrics()['consumer-fetch-manager-metrics']['records-consumed-rate'])


conn.commit()
print("Records inserted........")

# Closing the connection
conn.close()
time2 = time.time()
timedeltaa = time2 - time1
print(f'Total time taken for Records: {timedeltaa*1000} ms')