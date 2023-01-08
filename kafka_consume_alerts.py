#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install kafka-python


# In[1]:


pip install boto3


# In[1]:


from kafka import KafkaConsumer
import boto3
import json
from json import dumps


# In[3]:


#Setting up kafka server configurations
bootstrap_servers = ['localhost:9092']
topicName = 'HealthAlerts'

#setting up sns configurations
client = boto3.client('sns',region_name='us-east-1',aws_access_key_id="AKIAYQR5HNKQK7OITP67",aws_secret_access_key="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")



# In[4]:


# Defining Kafka consumer
consumer = KafkaConsumer(topicName,bootstrap_servers="localhost:9092",auto_offset_reset='earliest',value_deserializer=lambda m: str(m, 'UTF-8'))



# In[ ]:


# Publish messages to SNS topic
for msg in consumer:
    response = client.publish(TopicArn='arn:aws:sns:us-east-1:585317706400:HealthMonitoring', Message=msg.value )
    print(response)


# In[ ]:




