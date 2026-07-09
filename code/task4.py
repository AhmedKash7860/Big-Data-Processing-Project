import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.streaming import DataStreamWriter, DataStreamReader
from pyspark.sql.functions import explode ,split ,window , regexp_extract , to_timestamp , col , when , count , current_timestamp
from pyspark.sql.types import IntegerType, DateType, StringType, StructType
from pyspark.sql.functions import sum,avg,max

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("HDFSSparkStreaming")\
        .getOrCreate() \

    spark.sparkContext.setLogLevel("ERROR")
#Create a streaming DataFrame
    logsDF = (spark.readStream
        .format("socket")
        .option("host", os.environ["STREAMING_SERVER_HDFS_2K"])
        .option("port", int(os.environ["STREAMING_SERVER_HDFS_2K_PORT"]))
        .load())

#question 1
#derive columns using regular expressions
    df = (logsDF
    .withColumn("timestamp", to_timestamp(regexp_extract(col("value"), r"(\d{6} \d{6})", 0), "MMddyy HHmmss"))
    .withColumn("level", regexp_extract(col("value"), r"(INFO|WARN|ERROR)", 0))
    .withColumn("component", regexp_extract(col("value"), r"(DataNode|FSNamesystem)", 0))
    .withColumn("host", regexp_extract(col("value"), r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})", 0))
    .withColumn("message", regexp_extract(col("value"), r": (.*)", 1))
    .withColumn("malformed", when((col("timestamp").isNotNull()) & (col("level").isNotNull()), "No").otherwise("Yes")))
    

#print schema
    df.printSchema()
    
      
#malformed and unmalformed queries
    is_malformed = df.filter(col("malformed") == "Yes")
    isnot_malformed = df.filter(col("malformed") == "No")

#run malformed and unmalformed queries one by one
    query = isnot_malformed.writeStream.format("console") \
    .outputMode("append") \
    .option("truncate" , "false") \
    .start()

#question 2
#appy event time watermark and set window duration
    
    df2 = df.withWatermark("timestamp" , "5 seconds").groupBy(window(col("timestamp") , "60 seconds" ,"30 seconds")).count()
    
#run query
    query = df2.writeStream.format("console") \
    .outputMode("append") \
    .option("truncate" , "false") \
    .start()

#question 3a
#filter where component includes datanode and set window duration and slide interval and count occurences per window
    df3 = df.filter(col("component") == "DataNode").groupBy(window(col("timestamp") , "60 seconds" ,"30 seconds")).count()
#run query
    query = df3.writeStream.format("console") \
    .outputMode("update") \
   .option("truncate" , "false") \
    .start()

#question 3b
#group by host with watermark and order by count in descending order
    
    df3b = df.withWatermark("timestamp" ,"7 seconds").groupBy("host").count().orderBy(col("count").desc())
    
#run query
    query = df3b.writeStream.format("console") \
    .outputMode("complete") \
    .option("truncate" , "false") \
    .start()

#question 4
#filter logs , group by host and compute count
    
    df4 = df.filter((col("level") == "INFO") & (col("message").contains("blk_"))).groupBy("host").count() \
            .withColumn("time stamp" , current_timestamp())
    
#run query
    query = df4.writeStream.format("console") \
    .trigger(processingTime="15 seconds") \
    .outputMode("complete") \
    .option("truncate" , "false") \
    .start()



#question 5
#create a checkpoint directory
    bucketName = os.environ["BUCKET_NAME"]
    checkPoint = "s3a://"+bucketName+"/task4/q5"
    
#reuse qs 3b query 
    query = df3b.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", "false") \
    .option("checkpointLocation",checkPoint ) \
    .start()
    

#only ran 1 query at a time. Commented out the queries not being executed.

    query.awaitTermination()
