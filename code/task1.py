import os
from datetime import datetime
from pyspark.sql import SparkSession , Window
from pyspark.sql.functions import col, to_timestamp, date_format, count , hour , dayofweek , round , when , row_number , sum


if __name__ == "__main__":

    spark = SparkSession.builder.appName("task1").getOrCreate()

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
    
# load data into dataframe
    df = spark.read.json("s3a://" + s3_data_repository_bucket + "/ECS765/gharchive/2024-10/*.json.gz")
    
# derive date , hour and dow
    df = df.withColumn("created_at_ts", to_timestamp(col("created_at"))) \
           .withColumn("date", date_format(col("created_at_ts"), "yyyy-MM-dd")) \
           .withColumn("hour" , hour(col("created_at_ts"))) \
           .withColumn("dow" , dayofweek(col("created_at_ts")))
    
#question 1
    df.cache()

#calculate summary metrics
    rowCount = df.count()
    eventTypes = df.select("type").distinct().count()
    repo = df.select("repo.id").distinct().count()
    actor = df.select("actor.id").distinct().count()
    
#create dataframe for all summary metrics
    summary = spark.createDataFrame([("Row Count" , rowCount) , ("Distinct event types" , eventTypes) , ("Distinct repositories" , repo) , ("Distinct actors" , actor)] , ["Summary Metric" , "Output"])
    
#load summary metrics into a csv file
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary.coalesce(1).write.csv(f"s3a://{s3_bucket}/task1_output_{now}.csv", header=True)
    
#prints the schema , row count and a preview of 10 rows
    df.printSchema()
    print("Row count" , rowCount)
    df.show(10)

#question 2
    
#group by dow and compute total events and share %
    dayCount = df.groupby("dow").count().withColumnRenamed("count" , "Event Count")
    dayCount = dayCount.withColumn("Share (%)" , round(col("Event Count") / rowCount * 100 , 1))
    
#assign correct numbers to days of week e.g. Monday = 1 . In Spark , Sunday = 1
    dayCount = dayCount.withColumn("dayNumber" , when(col("dow") == 1 , 7) \
                                   .when(col("dow") == 2 , 1) \
                                   .when(col("dow") == 3 , 2) \
                                   .when(col("dow") == 4 , 3) \
                                   .when(col("dow") == 5 , 4) \
                                   .when(col("dow") == 6 , 5) \
                                   .when(col("dow") == 7 , 6))

#replace weekday numbers with actual weekday names
    dayCount = dayCount.withColumn("Day of Week" , when(col("dayNumber") == 1 , "Monday") \
                                   .when(col("dayNumber") == 2 , "Tuesday") \
                                   .when(col("dayNumber") == 3 , "Wednesday") \
                                   .when(col("dayNumber") == 4 , "Thursday") \
                                   .when(col("dayNumber") == 5 , "Friday") \
                                   .when(col("dayNumber") == 6 , "Saturday") \
                                   .when(col("dayNumber") == 7 , "Sunday"))

#select columns to display in csv file
    dayCount = dayCount.orderBy("dayNumber").select("Day of Week" , "Event Count" , "Share (%)")

#load data into a csv file
    dayCount.coalesce(1).write.csv(f"s3a://{s3_bucket}/task2_output_{now}.csv", header=True)

#question 3a

#only keep watchEvent and PullRequest events and group by repo full name, type, count events
    popularity = df.filter(col("type").isin("WatchEvent" , "PullRequestEvent"))
    finalPopularity = popularity.groupBy(col("repo.name").alias("Repository") , col("type").alias("Event Type")).count() 
    
#label each row with a number 
    rowLabel = Window.orderBy(col("count").desc())
    finalPopularity = finalPopularity.withColumn("#", row_number().over(rowLabel))

#select columns to display in csv file
    finalPopularity = finalPopularity.select(
        col("#"),
        col("Event Type"),
        col("Repository"),
        col("count")
        )

#order by event count and get top 10

    finalPopularity = finalPopularity.orderBy(col("count").desc()).limit(10)

#load data into a csv file
    finalPopularity.coalesce(1).write.csv(f"s3a://{s3_bucket}/task3a_output_{now}.csv", header=True)

#question 3b

#Count distinct actors per repo for both event types
    
    actorDiversity = popularity.select(
        col("repo.name").alias("Repository"),
        col("type").alias("Event Type"),
        col("actor.id")).distinct().groupBy("Repository" , "Event Type").count() \
        .withColumnRenamed("count" , "Distinct Actors")

#label each row with a number 
    rowLabel = Window.orderBy(col("Distinct Actors").desc())
    actorDiversity = actorDiversity.withColumn("#", row_number().over(rowLabel))

#select columns to display in csv file
    actorDiversity = actorDiversity.select(
        col("#"),
        col("Repository"),
        col("Event Type"),
        col("Distinct Actors"))

#order by actor diversity
    actorDiversity = actorDiversity.orderBy(col("Distinct Actors").desc())

#load data into a csv file
    actorDiversity.coalesce(1).write.csv(f"s3a://{s3_bucket}/task3b_output_{now}.csv", header=True)

# question 4

#Assign first event and Filter where row number = 1    
    

    firstEvent = Window.partitionBy(col("actor.id")).orderBy(col("created_at_ts"))
    newContributors = df.withColumn("rowNumber" , row_number().over(firstEvent)) \
    .filter(col("rowNumber") == 1)

#Group by repo full name and count distinct new actors
    
    newContributors = newContributors.select(
        col("repo.name").alias("Repository"),
        col("actor.id")).distinct().groupBy("Repository").count() \
        .withColumnRenamed("count" , "New Contributors")
    
#label each row with a number
    rowLabel = Window.orderBy(col("New Contributors").desc())
    newContributors = newContributors.withColumn("#", row_number().over(rowLabel))

#select columns to display in csv file
    
    newContributors = newContributors.select(
        col("#"),
        col("Repository"),
        col("New Contributors"))

#order by new contributors and get top 10
    newContributors = newContributors.orderBy(col("New Contributors").desc()).limit(10)

#load data into a csv file
    newContributors.coalesce(1).write.csv(f"s3a://{s3_bucket}/task4_output_{now}.csv", header=True)

#question 5
    
#Filter PushEvent , Extract commit count from payload.size and Group by hour; sum commits.
    
    timeofDay = df.filter(col("type") == "PushEvent").groupBy("hour").sum("payload.size")\
    .withColumnRenamed("sum(payload.size AS `size`)" , "Total Commits")

#select columns to display in csv file
    timeofDay = timeofDay.select(
        col("hour").alias("Hour (UTC)"),
        col("Total Commits"))

#order by the hour 0-23

    timeofDay = timeofDay.orderBy("Hour (UTC)")

#load data into a csv file
    timeofDay.coalesce(1).write.csv(f"s3a://{s3_bucket}/task5_output_{now}.csv", header=True)

#question 6

#Filter non-null organisations
    
    organisation = df.filter(col("org").isNotNull())

#Group by org.login and count

    organisation = organisation.groupBy(col("org.login")).count() \
    .withColumnRenamed("count" , "Event Count")

#select columns to display in csv file
    organisation = organisation.select(
        col("login").alias("Org"),
        col("Event Count"))

#order by event count and get top 5
    organisation = organisation.orderBy(col("Event Count").desc()).limit(5)

#load data into a csv file
    organisation.coalesce(1).write.csv(f"s3a://{s3_bucket}/task6_output_{now}.csv", header=True)
    
                             

    


    spark.stop()

