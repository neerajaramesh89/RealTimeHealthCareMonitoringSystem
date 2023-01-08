#!/usr/bin/env python
# coding: utf-8

# In[6]:


pip install kafka-python


# In[1]:


import mysql.connector

from kafka import KafkaProducer

import time
import json

from json import dumps


# In[2]:


#Setting up the producer configurations
bootstrap_servers = ['localhost:9092']
topicName = 'vitals'
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x:dumps(x).encode('utf-8'))


# In[3]:


#setting connection with RDS server
connection = mysql.connector.connect(host='upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com', database='testdatabase',user='student', password='STUDENT123')

 
cursor =connection.cursor()

cursor.execute("SELECT * FROM patients_vital_info")

rows = cursor.fetchall()

#read each row and push to kafka topic
for row in rows:
    time.sleep(1)
    # send messages to kafka producer
    producer.send(topicName, {'customerId': row[0],'heartBeat' : row[1],'bp' : row[2]})
    #ack = producer.send(topicName, {'customerId': row[0],'heartBeat' : row[1],'bp' : row[2]})
    #metadata = ack.get()
    #print(metadata.topic)
    #print(metadata.partition)


# In[8]:


# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer

# Import sys module
import sys
import time
import json

from json import dumps

# Define topic name from where the message will be recieved
topicName = 'vitals'

# Define server with port
consumer = KafkaConsumer(topicName,bootstrap_servers="localhost:9092",auto_offset_reset='earliest')


# Read and print message from consumer
                                                                                                                          
for msg in consumer:
                                                                                                                          print("Topic Name=%s,Message=%s"%(msg.topic,msg.value))


# In[ ]:




