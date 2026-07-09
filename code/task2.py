import os
from datetime import datetime
from pyspark.sql import SparkSession , Window
from pyspark.sql.functions import col, to_timestamp, date_format, month, count ,hour , min , max , row_number , concat_ws, when , expr , stddev ,mean , avg , round , abs , coalesce


if __name__ == "__main__":
    spark = SparkSession.builder.appName("task2").getOrCreate()

    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
#load data from csv files
    df = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/tfl/cyclehire/2024-q3/*.csv", header=True, inferSchema=True)
#question 1
#parse timestamps to obtain date hour and month and ensure correct data types
    df = df.withColumn("StartTime", coalesce(
        to_timestamp(col("Start date"), "yyyy-MM-dd HH:mm"),
        to_timestamp(col("Start date"), "dd/MM/yyyy HH:mm"))) \
           .withColumn("month", month(col("StartTime"))) \
           .withColumn("date" , date_format(col("StartTime"), "yyyy-MM-dd")) \
           .withColumn("hour" , hour(col("StartTime"))) \
           .withColumn("duration" , (col("Total duration (ms)") / 1000).cast("int"))
    
#remove records where station ids are missing or duration is negative/nulll
    df = df.filter(
    (col("duration").isNotNull()) &
    (col("duration") >= 0) &
    (col("Start station number").isNotNull()) &
    (col("End station number").isNotNull()))

#cache the cleaned dataframe
    df.cache()

#summary statistics
    rowCount = df.count()
    distinctStart = df.select("Start station").distinct().count()
    distinctEnd = df.select("End station").distinct().count()
    minDuration = df.select(min("duration")).collect()[0][0]
    maxDuration = df.select(max("duration")).collect()[0][0]

    minDate = df.select(min("date")).collect()[0][0]
    maxDate = df.select(max("date")).collect()[0][0]

    dateRange = str(minDate) + " to " + str(maxDate)
#create dataframe
    bikeSummary = spark.createDataFrame([("Total Rows" , str(rowCount)) , ("Distinct Start stations" , str(distinctStart)) , ("Distinct end stations" , str(distinctEnd)) , ("Min Duration" , str(minDuration)) , ("Max Duration" , str(maxDuration)) , ("Date Range" , str(dateRange))] , ["Metric" , "Values"])

#output data to a csv file
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    bikeSummary.coalesce(1).write.csv(f"s3a://{s3_bucket}/task2_output_qs1{now}.csv", header=True)

#schema and row count
    df.printSchema()
    print("Row count" , rowCount)


#question 2a
#set k to 5
    k = 5
#get month in correct format and group by start station name and number
    stationPopularity = df.withColumn("month" , date_format(col("StartTime"), "MMM"))
    stationPopularity = stationPopularity.groupBy(col("Start station") , col("Start station number") , col("month")).count() \
    .withColumnRenamed("count" , "Trip Starts")
#rank based on trip starts
    rowLabel = Window.partitionBy(col("month")).orderBy(col("Trip Starts").desc())
    stationPopularity = stationPopularity.withColumn("Rank", row_number().over(rowLabel))
#only keep top 5
    stationPopularity = stationPopularity.filter(col("Rank") <= k)

#select columns
    stationPopularity = stationPopularity.select(
        col("month"),
        col("Start station number").alias("Station ID"),
        col("Start station"),
        col("Trip Starts"),
        col("Rank"))
#order by trip starts and write to csv file
    stationPopularity = stationPopularity.orderBy(col("Trip Starts").desc())
    stationPopularity.coalesce(1).write.csv(f"s3a://{s3_bucket}/task2_output_qs2a{now}.csv", header=True)
    

#question 2b
#concatenate start station with end station
    route = df.withColumn("Route" , concat_ws(" -> " , col("Start station") , col("End station")))
#filter out self loops
    route = route.filter(col("Start station") != col("End station"))
#calculate number of trips
    route = route.groupBy(col("Route")).count().withColumnRenamed("count" , "Trips")
#label each row
    rowLabel = Window.orderBy(col("Trips").desc())
    route = route.withColumn("#", row_number().over(rowLabel))
#select columns
    route = route.select(
        col("#"),
        col("Route"),
        col("Trips"))
#order by trips
    route = route.orderBy(col("Trips").desc()).limit(20)
#write data to a csv file
    route.coalesce(1).write.csv(f"s3a://{s3_bucket}/task2_output_qs2b{now}.csv", header=True)

#question3
#create hour based time of day buckets
    timeofDay = df.withColumn("time_of_day" , when((col("hour") > 4) & (col("hour") < 12) , "Morning") \
                              .when(((col("hour") > 11) & (col("hour") < 17)) , "Afternoon") \
                              .when(((col("hour") > 16) & (col("hour") < 22)) , "Evening") \
                              .otherwise("Night"))

#aggregate the total number of trips , median duration and 90th percentile
    timeofDay = timeofDay.groupBy(col("time_of_day")).agg(
        count("*").alias("Trips"),
        expr("percentile_approx(`duration` , 0.5)").alias("Median (s)"),
        expr("percentile_approx(`duration` , 0.9)").alias("90th Percentile (s)"))

#order results by time of day order

    timeofDay = timeofDay.withColumn("timeNumber" , when(col("time_of_day") == "Morning" , 1) \
                                     .when(col("time_of_day") == "Afternoon" , 2) \
                                     .when(col("time_of_day") == "Evening" , 3) \
                                     .when(col("time_of_day") == "Night" , 4 ))
#select columns and order by timeNumber
    timeofDay = timeofDay.select(
        col("time_of_day"),
        col("Trips"),
        col("Median (s)"),
        col("90th Percentile (s)"))

    timeofDay = timeofDay.orderBy(col("timeNumber"))
#output data to a csv file
    timeofDay.coalesce(1).write.csv(f"s3a://{s3_bucket}/task2_output_qs3{now}.csv", header=True)

#question 4
#only keep values for August
    dailyScan = df.filter(col("month") == 8)
#compute average computation and trip count
    dailyScan = dailyScan.groupBy(col("date")).agg(
        round(avg("duration")).alias("Avg Duration (s)"),
        count("*").alias("Trip Count"))
#create a partition Window
    window = Window.partitionBy()
#compute z-scores
    dailyScan = dailyScan.withColumn("z-score(duration)" , round((col("Avg Duration (s)") - mean("Avg Duration (s)").over(window)) / stddev("Avg Duration (s)").over(window) , 2))

    dailyScan = dailyScan.withColumn("z-score(trip count)" , round((col("Trip Count") - mean("Trip Count").over(window)) / stddev("Trip Count").over(window) , 2))

#determine if values are anomalies
    dailyScan = dailyScan.withColumn("Anomaly?" , when((col("z-score(duration)") > 2) | (col("z-score(trip count)") < -2) , "Yes") \
    .otherwise("No"))
#select columns
    dailyScan.select(
        col("date"),
        col("Avg Duration (s)"),
        col("Trip Count"),
        col("z-score(duration)"),
        col("z-score(trip count)"),
        col("Anomaly?"))
#outputdata to a csv file
    dailyScan.coalesce(1).write.csv(f"s3a://{s3_bucket}/task2_output_qs4{now}.csv", header=True)

#question 5
    #morning Peak Window
#only keep rows for morning peak window
    morning = df.filter((col("hour") > 6) & (col("hour") < 11))

#calculate number of starts
    morningStart = morning.groupBy(col("Start station")).count() \
                          .withColumnRenamed("count" , "Starts")
#calculate number of ends
    morningEnd = morning.groupBy(col("End station")).count() \
                        .withColumnRenamed("count" , "Ends")

#outer join morning start with morning end to get station names
    morningPeak = morningStart.join(
        morningEnd ,
        morningStart["Start station"] == morningEnd["End station"] ,
        "outer" )
#derive net flow
    morningPeak = morningPeak.withColumn("Net Flow" , col("Starts") - col("Ends"))

#row label for + net flow table
    rowLabel = Window.orderBy(abs(col("Net Flow")).desc())
   
    morningInflow = morningPeak.filter(col("Net Flow") > 0) \
                               .withColumn("#" , row_number().over(rowLabel))
                               
#select column
    morningInflow = morningInflow.select(
        col("#") ,
        col("Start station").alias("Station Name") ,
        col("Starts") ,
        col("Ends") ,
        col("Net Flow")).orderBy(abs("Net Flow").desc()).limit(10)
    
#row label for - net flow table
    
    morningOutflow = morningPeak.filter(col("Net Flow") < 0) \
                                .withColumn("#" , row_number().over(rowLabel)) 
                                
#select columns to get top 10
    morningOutflow = morningOutflow.select(
        col("#") ,
        col("Start station").alias("Station Name") ,
        col("Starts") ,
        col("Ends") ,
        col("Net Flow")).orderBy(abs("Net Flow").desc()).limit(10)
 
#output data to a csv file
    morningInflow.coalesce(1).write.csv(f"s3a://{s3_bucket}/task2_output_qs5morninginflow{now}.csv", header=True)
    morningOutflow.coalesce(1).write.csv(f"s3a://{s3_bucket}/task2_output_qs5morningoutflow{now}.csv", header=True)

#evening peak window
#only keep rows for evening peak window
    evening = df.filter((col("hour") > 15) & (col("hour") < 20))
#calculate number of starts

    eveningStart = evening.groupBy(col("Start station")).count() \
                          .withColumnRenamed("count" , "Starts")
#calculate number of ends
    eveningEnd = evening.groupBy(col("End station")).count() \
                        .withColumnRenamed("count" , "Ends")
#outer join evening start with evening end to get station names
    eveningPeak = eveningStart.join(
        eveningEnd ,
        eveningStart["Start station"] == eveningEnd["End station"] ,
        "outer" )
#derive net flow
    eveningPeak = eveningPeak.withColumn("Net Flow" , col("Starts") - col("Ends"))

#row label for + net flow table 
    rowLabel = Window.orderBy(abs(col("Net Flow")).desc())
   
    eveningInflow = eveningPeak.filter(col("Net Flow") > 0) \
                               .withColumn("#" , row_number().over(rowLabel))
                               
#select column
    eveningInflow = eveningInflow.select(
        col("#") ,
        col("Start station").alias("Station Name") ,
        col("Starts") ,
        col("Ends") ,
        col("Net Flow")).orderBy(abs("Net Flow").desc()).limit(10)

#row label for - net flow tabl   
    eveningOutflow = eveningPeak.filter(col("Net Flow") < 0) \
                                .withColumn("#" , row_number().over(rowLabel)) 
                                
#select column
    eveningOutflow = eveningOutflow.select(
        col("#") ,
        col("Start station").alias("Station Name") ,
        col("Starts") ,
        col("Ends") ,
        col("Net Flow")).orderBy(abs("Net Flow").desc()).limit(10)
 
#output data to a csv file
    eveningInflow.coalesce(1).write.csv(f"s3a://{s3_bucket}/task2_output_qs5eveninginflow{now}.csv", header=True)
    eveningOutflow.coalesce(1).write.csv(f"s3a://{s3_bucket}/task2_output_qs5eveningoutflow{now}.csv", header=True)



    
    spark.stop()
    





  


