# RealTimeHealthCareMonitoringSystem
Build a reliable data pipeline solution to store and analyse a stream of real-time data flowing from various IoT devices at hospitals and health centres and send alerts in case of any discrepancy.
The project comprises of the following tasks.
1. Push data from RDS into Kafka topic.
2. Build a Kafka topic to collect a stream of records from an application.
3. Build a Spark application to collect messages from the Kafka queue into the HDFS file system.
4. Create an HBase table and insert data
5. Build a Sqoop application to extract data from the RDS instance into a Hive table.
6. Build a Spark application to compare two sets of data and send messages to target a Kafka topic.
7. Build an application to send SNS email notifications.
