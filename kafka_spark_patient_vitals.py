#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#writing data in parquet format
from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from pyspark.sql import functions 

#creating spark context
spark=SparkSession         .builder         .appName("PatientVitals")         .getOrCreate() 
spark.sparkContext.setLogLevel('ERROR') 
 
# define schema for dataframe
vitalsSchema = StructType().add("customerId", IntegerType())                     .add("heartBeat",IntegerType())                    .add("bp",IntegerType())
                   
#read data from kafka server                  
lines=spark         .readStream         .format("kafka")         .option("kafka.bootstrap.servers","54.160.142.96:9092")         .option("subscribe","vitals")        .option("startingOffsets", "earliest")         .load() 

#add timestamp column from kafka message 
vitals_df=lines.selectExpr("CAST(timestamp AS STRING) as Message_time", "CAST(value AS STRING) as json").select(functions.from_json(functions.col("json"), vitalsSchema).alias("vitalData"),col("Message_time")) 
 
#cast into timestamp type
vitals_df=vitals_df.select(col("vitalData.*"),col("Message_time").cast("timestamp")) 

#write to parquet file
vitalsParquet=vitals_df.writeStream    .format("parquet")    .option("path", "patientVitals")    .option("checkpointLocation", "patientVitals")     .start()

#waiting for source to terminate
vitalsParquet.awaitTermination()

