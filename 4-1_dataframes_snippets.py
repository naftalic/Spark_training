"""
Code fragments from Lesson 4.1, `Working with dataframes'
Here we work with the json formatted file that results from
running the pgcat.py script supplied in week 3.
If you don't already have it, it's supplied with this week's resources.
"""

# read lines of the catalogue index into an RDD, filtering out `None' values
rdd = sc.textFile('s3n://BUCKET_NAME/results/pgcat.json/part-00000').filter(lambda x: x != 'None')
# return the first 2 elements in the RDD
rdd.take(2)
# Spark <=1.6/backwards compatible way to instantiate an SQLContext object
sqlContext = SQLContext(sc)
# Spark <=1.6 way to turn JSON data into a dataframe
df1 = sqlContext.jsonRDD(rdd)
# Spark >=2.0 way to turn JSON data into a dataframe
df1 = spark.read.json(rdd)
# inspect dataframe
df1.show()
# look at first row in dataframe (returns Row object)
df1.first()
# register dataframe as table for SQL style querying (Spark <=1.6)
df1.registerTempTable("pgcat")
# register dataframe as table for SQL style querying (Spark >=2.0)
df1.createOrReplaceTempView("pgcat")
# query dataframe (Spark <=1.6)
df2 = sqlContext.sql("SELECT id, title, author FROM 'pgcat' WHERE lang='en' LIMIT 5")
# query dataframe (Spark >=2.0)
df2 = spark.sql("SELECT id, title, author FROM 'pgcat' WHERE lang='en' LIMIT 5")
