"""
Here are the fragmentsfrom Lesson 2.2, `Using Spark methods to read and write data'
The examples here assume you have a copy of our subset stored in a bucket on S3.
"""

# create an RDD from a text file. each line is an element.
rdd = sc.textFile("s3n://AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY@BUCKET_NAME/gutenberg_dataset/1/8/0/1804/1804.txt")

# look for lines in the RDD with the word `mankind' in, and count them
rdd.filter(lambda line: "mankind" in line).count()

# repeat the above, but saving result as a text file
rdd.filter(lambda line: "mankind" in line).saveAsTextFile("s3n://AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY@BUCKET_NAME/result/lesson_2-2")

# read multiple files to an RDD (still the elements are lines)
rdd = sc.textFile("s3n://AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY@BUCKET_NAME/1/8/*/*")

# read whole text files as key-value pairs (key: object path, value: file content)
rdd = sc.wholeTextFiles("s3n://AWS_ACCESS_KEY_ID:AWS_SECRET_ACCESS_KEY@BUCKET_NAME/1/8/*/*")

# create an RDD from just the keys
keys = rdd.map( lambda x:x[0] )
keys.count()
keys.collect()
