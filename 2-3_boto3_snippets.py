"""
Here are a few boto3 fragments you might find useful!
For more info, refer to: http://boto3.readthedocs.io/en/latest/

Before running these fragments, you must first configure 
your aws access credentials by creating a default profile in ~/.aws/credentials
"""

from pyspark import SparkContext
import boto3, re

WORKINGDIR = 's3n://YOUR_BUCKET/' # path to bucket on s3 where you'll save your results
BUCKET = 'YOUR_BUCKET' # name of your bucket

# create session object
session = boto3.Session(profile_name='default')

# create a resource instance (recommended interface to AWS)
s3 = boto3.resource('s3')

# retrieve a specific bucket
bucket = s3.Bucket(BUCKET)

# create a collection object, filtering by prefix
objs = bucket.objects.filter(Prefix='gutenberg_dataset/1')

# print the keys of objects in the collection
for obj in objs:  
	print(obj.key)

# create client instance (lower-level alternative to resource)
s3_client = session.client('s3')

# check if a bucket exists
response = s3_client.list_buckets()
buckets = [x.get('Name') for x in response.get('Buckets')]
if BUCKET in buckets:
    print('Bucket exists')

	# fetch an object by key
	obj = s3_client.get_object(
		Bucket=BUCKET,
		Key='gutenberg_dataset/7/0/700/700.txt',
	)

	# get all objects in a bucket, filtering by prefix
	response = s3_client.list_objects(
        Bucket=BUCKET,
        Prefix='gutenberg_dataset/meta/')

	# create a list of filenames from the object keys
	filenames = [ x.get('Key') for x in response.get('Contents') if re.search('.rdf$', x.get('Key')) ]

