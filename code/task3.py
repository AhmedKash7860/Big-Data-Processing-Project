import os
from datetime import datetime
from pyspark.sql import SparkSession , Window , Row
from graphframes import GraphFrame
from pyspark.sql.functions import col , count , row_number , concat_ws , sum , countDistinct , explode, array_repeat

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("task3")
        .config("spark.jars.packages", "graphframes:graphframes:0.8.3-spark3.5-s_2.12")
        .getOrCreate()
    )

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

    #Set checkpoint dir
    checkpoint_dir = f"s3a://{s3_bucket}/spark_checkpoints"
    spark.sparkContext.setCheckpointDir(checkpoint_dir)


    airports = "s3a://" + s3_data_repository_bucket + "/ECS765/openflights/airports.csv"
    routes = "s3a://" + s3_data_repository_bucket + "/ECS765/openflights/routes.csv"

    # # TODO: change column name if needed
    
    v = spark.read.csv(airports, header=True, inferSchema=True).withColumnRenamed("Airport ID", "id")
    e = spark.read.csv(routes, header=True, inferSchema=True).withColumnRenamed(" source airport id", "src").withColumnRenamed(" destination airport id", "dst")

#question 1
    
#calculate number of raw vertices and raw edges
    vrowCount = v.count()
    erowCount = e.count()

# de duplicate vertices   
    cleanV = v.dropDuplicates(["id"])

# select required vertices columns and cast to appropriate data types    
    cleanV = cleanV.select(
        col("id").cast("int"),
        col("Name"),
        col("City"),
        col("Country"),
        col("IATA"),
        col("Latitude").cast("double"),
        col("Longitude").cast("double"))

#filter out rows with invalid geographical values
    cleanV = cleanV.filter((col("Latitude").isNotNull()) & (col("Longitude").isNotNull()))
#calculate number of clean vertices
    cleanvrowCount = cleanV.count()

#select required edges columns and cast to appropriate data types
    cleanE = e.select(
       col("src").cast("int"),
       col("dst").cast("int"),
       col("airline"),
       col(" stops"))

#load id's for all airports in vertices into a list
    airportId = [airport.id for airport in cleanV.select("id").collect()]

#remove all endpoint airports which are not in the list
    cleanE = cleanE.filter((col("src").isin(airportId)) & (col("dst").isin(airportId)))
    
#calculate number of clean edges
    cleanerowCount = cleanE.count()

#calculate how many edges were removed due to not being in the vertices dataframe
    edgesRemoved = erowCount - cleanerowCount


#create dataframe
    summary = spark.createDataFrame([("Raw Vertices" , vrowCount) , ("Clean Vertices" , cleanvrowCount) ,
                                     ("Raw Edges" , erowCount) , ("Edges Removed (no endpoint)" , edgesRemoved) ,
                                     ("Clean Edges" , cleanerowCount)] , ["Metric" , "Value"])
                            
#create graph
    g = GraphFrame(cleanV, cleanE)

#output data to a csv file
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary.coalesce(1).write.csv(f"s3a://{s3_bucket}/task3_output_qs1{now}.csv", header=True)

#show sample tables for vertices and edges
    cleanV.show(7)
    cleanE.show(7)

#question 2a
#calculate the number of connected components
    conComponents = g.connectedComponents()

#group by component column created by the function and calculate the number of rows
    componentData = conComponents.groupBy(col("component")).count() \
                                                           .withColumnRenamed("count" , "Size") \
                                                           .withColumnRenamed("component" , "Component ID")
#calculate the number of rows and order by the size descensingly to calculate the top 5
    rowCount = componentData.count()
    print("The number of connected components are " , rowCount)

    componentData = componentData.orderBy(col("Size").desc()).limit(5)
#output data to a csv file
    componentData.coalesce(1).write.csv(f"s3a://{s3_bucket}/task3_output_qs2a{now}.csv", header=True)

#question 2b
#total degree
#calculate indegree and out degree and rename id columns so no ambiguity error
    inDegree = g.inDegrees.withColumnRenamed("id" , "in_id")
    outDegree = g.outDegrees.withColumnRenamed("id" , "out_id")

#join the 3 dataframes together
    totalDegree = cleanV.join(inDegree , cleanV["id"] == inDegree["in_id"] , "left")
    totalDegree = totalDegree.join(outDegree , cleanV["id"] == outDegree["out_id"] , "left")
    totalDegree = totalDegree.withColumn("Total Degree" , col("inDegree") + col("outDegree")) \
                             .withColumnRenamed("Name" , "Airport")
  

#add a row label
    rowLabel = Window.orderBy(col("Total Degree").desc())
    totalDegree = totalDegree.withColumn("#", row_number().over(rowLabel))
#select columns
    totalDegree = totalDegree.select(
        col("#"),
        col("Airport"),
        col("Total Degree"))
#order by total degree to get top 10
    totalDegree = totalDegree.orderBy(col("Total Degree").desc()).limit(10)
    
#triangle count
#calculate triangle count and rename columns so no ambiguity error
    triangle = g.triangleCount().withColumnRenamed("Name" , "Airport") \
                                .withColumnRenamed("count" , "Triangle Count")
#join the 2 dataframes
    triangleParticipation = cleanV.join(triangle, cleanV["id"] == triangle["id"])     
#add a row label
    rowLabel = Window.orderBy(col("Triangle Count").desc())
    triangleParticipation = triangleParticipation.withColumn("#", row_number().over(rowLabel))

#select columns
    triangleParticipation = triangleParticipation.select(
        col("#"),
        col("Airport"),
        col("Triangle Count"))

#order by triangle count to get top 10
    triangleParticipation = triangleParticipation.orderBy(col("Triangle Count").desc()).limit(10)

#output data to a csv file
    totalDegree.coalesce(1).write.csv(f"s3a://{s3_bucket}/task3_output_qs2bdegree{now}.csv", header=True)
    triangleParticipation.coalesce(1).write.csv(f"s3a://{s3_bucket}/task3_output_qs2btriangle{now}.csv", header=True)

#question 3
#get id's of start and end airport
    start = cleanV.filter(col("IATA") == "LHR").select("id").collect()[0]["id"]
    end = cleanV.filter(col("IATA") == "JFK").select("id").collect()[0]["id"]

#compute the breadt first search
    breadthSearch = g.bfs(
        fromExpr = "id = " + str(start),
        toExpr = "id = " + str(end),
        maxPathLength=4
    )
#select 1 BFS path
    finalPath = breadthSearch.limit(1)

#valid columns returned by the bfs
    validColumns = ["from" , "v1" , "v2" ,"v3", "to"]

# if bfs returns valid columns , load data for those columns into a list of Rows

    result =[]
    hopCount = 0
    for i  in range(len(validColumns)):
        if validColumns[i] in finalPath.columns:
            hop = finalPath.select(
                col(validColumns[i] + ".IATA").alias("Airport"),
                col(validColumns[i] + ".City").alias("City"),
                col(validColumns[i] + ".Country").alias("Country")).collect()[0]
            hopCount = hopCount + 1

            result.append(Row(Hop = hopCount , Airport = hop["Airport"] , City = hop["City"] , Country = hop["Country"])) 

#create data frame
    finalResult = spark.createDataFrame(result)
#output data to a csv file
    finalResult.coalesce(1).write.csv(f"s3a://{s3_bucket}/task3_output_qs3hops{now}.csv", header=True)

#store multiple different locations
    multidestinations = ["TUD" , "SLM" , "LHR" , "JFK" , "PEK" , "SYD" , "DXB"]

#get ids for those destinations
    destinations = cleanV.filter(col("IATA").isin(multidestinations)).select("id").collect()

    ids = []

    for id in destinations:
        ids.append(id["id"])

#compute the shortest Paths by passing in the id's of the destinations as a parameter
    shortestPath = g.shortestPaths(landmarks = ids)

#print schema and sample records
    shortestPath.printSchema()
    shortestPath.show(8)

#question 4a
#Apply PageRank Algorithm
    rank = g.pageRank(resetProbability=0.15 , maxIter = 15)
#store vertices dataframe
    pagerank = rank.vertices

#create airport column with IATA code and name and a row label
    pagerank = pagerank.withColumn("Airport" , concat_ws(" ", col("IATA") ,col("Name")))
    
#calculate total PageRanks to determine page rank probability of different airports
    PR = pagerank.agg(sum(col("pagerank"))).collect()[0][0]
    pagerank = pagerank.withColumn("pagerank", col("pagerank")/PR)
    
    rowLabel = Window.orderBy(col("pagerank").desc())
    pagerank = pagerank.withColumn("#", row_number().over(rowLabel))
#select columns
    pagerank = pagerank.select(
        col("#"),
        col("Airport"),
        col("pagerank").alias("PageRank Score"))
#order results
    pagerank = pagerank.orderBy(col("PageRank Score").desc()).limit(10)
#output data to a csv file
    pagerank.coalesce(1).write.csv(f"s3a://{s3_bucket}/task3_output_qs4a{now}.csv", header=True)
    
    
#question 4b
#group by source and destination airports and compute the weight
    weightedEdges = cleanE.groupBy("src" , "dst").agg(countDistinct("airline").alias("weight"))

    #create a new graph with weighted edges
    newGraph = GraphFrame(cleanV , weightedEdges)
    
#apply PageRank Algorithm
    pageRank2 = newGraph.pageRank(resetProbability=0.15, maxIter=15)
    
#store vertices dataframe
    pagerank2 = pageRank2.vertices

#create airport column with IATA code and name and a row label
    pagerank2 = pagerank2.withColumn("Airport" , concat_ws(" ", col("IATA") ,col("Name")))
    
#calculate total PageRanks to determine page rank probability of different airports
    PR2 = pagerank2.agg(sum(col("pagerank"))).collect()[0][0]
    pagerank2 = pagerank2.withColumn("pagerank", col("pagerank")/PR2)
    
    rowLabel = Window.orderBy(col("pagerank").desc())
    pagerank2 = pagerank2.withColumn("#", row_number().over(rowLabel))
#select columns
    pagerank2 = pagerank2.select(
        col("#"),
        col("Airport"),
        col("pagerank").alias("PageRank Score"))
#order results
    pagerank2 = pagerank2.orderBy(col("PageRank Score").desc()).limit(10)
#output data to a csv file
    pagerank2.coalesce(1).write.csv(f"s3a://{s3_bucket}/task3_output_qs4b{now}.csv", header=True)

#question 5
#Apply the community detection algorithm
    community = g.labelPropagation(maxIter = 10)
    community.show()
#number of detected communities
    communityCount = community.select("label").distinct().count()
    print("The number of communities are " , communityCount)

#Top-5 largest communities by size
    largestCommunities = community.groupBy(col("label"))\
    .agg(count("*").alias("Size"))
#select columns
    largestCommunities = largestCommunities.select(
        col("label").alias("Community ID"),
        col("Size"))
#order communities by size and show
    largestCommunities = largestCommunities.orderBy(col("Size").desc()).limit(5)
    largestCommunities.show()

#mapping community ID → geographic clusters.
    geoCluster = community.groupBy("label" , "Country").count()
#select columns and order by size
    geoCluster = geoCluster.select(
        col("label").alias("Community ID"),
        col("count").alias("Size"),
        col("Country").alias("Major Regions"))
    geoCluster = geoCluster.orderBy(col("Size").desc())
#write data to csv file
    geoCluster.coalesce(1).write.csv(f"s3a://{s3_bucket}/task3_output_qs5{now}.csv", header=True)

#for largest community , get top 10 routes inside the community
    largestCommunity = largestCommunities.collect()[0]["Community ID"]
#get id's of all airports in community
    airportCommunity = community.filter(col("label") == largestCommunity).select("id")
#insert all airport ids in list
    airports = [airport.id for airport in airportCommunity.collect()]
#only keep routes which have src / dst airports in list
    communityRoutes = cleanE.filter((col("src").isin(airports)) & (col("dst").isin(airports)))
#compute most popula routes in community
    popularRoutes = communityRoutes.groupBy("src" , "dst").count().orderBy(col("count").desc()).limit(10)
    
#output data
    popularRoutes.show()


    
    
    spark.stop()


    
    
    
    
    
    


