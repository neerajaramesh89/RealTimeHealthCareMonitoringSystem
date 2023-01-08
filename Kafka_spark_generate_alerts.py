#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.sql import functions

appName = "PySpark Example - Read Hive"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .enableHiveSupport() \
    .getOrCreate()

vitalsSchema = StructType().add("customerId", IntegerType()) \
                    .add("heartBeat",IntegerType())\
                    .add("bp",IntegerType())\
                    .add("Message_time",TimestampType())

spark.sparkContext.setLogLevel("WARN")

#read patient vitals
df=spark.readStream\
    .schema(vitalsSchema)\
    .parquet("/user/hadoop/patientVitals")
df.createOrReplaceTempView("Patients_Vital_Info")

#fetch details when alert flag is true
joinDF = spark.sql("select * from Patients_Vital_Info v, default.Patients_Contact_Info C,default.threshold_reference T where ((v.CustomerID == C.patientid) and (C.age between T.low_age_limit and T.high_age_limit) and T.alert_flag == 1 and ((T.attribute == 'heartBeat' and v.heartbeat between T.low_range_value and T.high_range_value ) or (T.attribute == 'bp' and v.bp between T.low_range_value and T.high_range_value))) ")
joinDF = joinDF.withColumn("alert_generated_time", current_timestamp())
joinDF = joinDF.withColumnRenamed("Message_time","Input_Message_time")

#send alert details with patients, vitals to kafka topic
alert_message=joinDF.select("patientname","age","patientaddress","phone_number","admitted_ward","bp","heartBeat","Input_Message_time","alert_generated_time","alert_message")
alert_message.selectExpr( "to_json(struct(*)) AS value")\
            .writeStream\
            .format("kafka")\
            .outputMode("append")\
            .option("kafka.bootstrap.servers", "44.213.22.182:9092")\
            .option("topic", "HealthAlerts")\
            .option("checkpointLocation", "Alerts1") \
            .start()\
            .awaitTermination()

