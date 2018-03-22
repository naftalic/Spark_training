"""
Example solution to week 4 peer assessment for Spark 2.0
Edit as required.
"""
from rdflib import Graph, Literal
from rdflib.namespace import Namespace, RDF, DCTERMS
from rdflib.term import bind

from pyspark.sql import SparkSession, SQLContext, Row
from pyspark import SparkContext

import sys, csv, io, json, boto3, re

""" Edit these variables! """
PGTERMS = Namespace(u'http://www.gutenberg.org/2009/pgterms/')
BUCKET = 'YOUR_BUCKET_NAME' # name of your bucket
PGPATH = 's3n://' + BUCKET + '/gutenberg_dataset/' # path where Gutenberg data resides
WORKINGDIR = 's3n://' + BUCKET + '/results/' # path where you are storing your results
PGCAT = WORKINGDIR + 'pgcat.json/part-00000' # path to your catalogue index (expects json)
bind(DCTERMS.RFC4646, str)


def cnt_pronouns(id,text):
    """ Count gender pronouns in a single ebook 
    Note: performs case-insensitive matching
    """
    mcount = len(re.findall(r'\b(he|him|himself|his)\b', str(text), re.IGNORECASE))
    fcount = len(re.findall(r'\b(she|her|herself|hers)\b', str(text), re.IGNORECASE))
    result = [id, mcount, fcount]
    return result

def get_keys(bucket_name):
    """ Returns all keys as list, excluding those in meta folder
    Note: list_objects can only return max 1000 objects, hence
    need to iterate through folders in data set
    (we know there's <1000 objects in each folder) """
    session = boto3.Session(profile_name='default')
    s3_client = session.client('s3')
    # check bucket exists
    response = s3_client.list_buckets()
    buckets = [x.get('Name') for x in response.get('Buckets')]
    if bucket_name in buckets:
        print('Bucket found. Proceeding to read contents...')
        s3keys = []
        for x in range(1,10):
            response = s3_client.list_objects(
                    Bucket=bucket_name,
                    Prefix=str(x)+'/',
                    )
            s3keys.extend([x.get('Key') for x in response.get('Contents')])
        return s3keys
    else:
        sys.exit('No bucket called {} found'.format(bucket_name))

def get_paths(ids, keys):
    """ Returns a list of paths derived from the s3 keys where
    a matching id is present in the list of ids. """
    paths = []
    for k in keys:
        id = get_id(k)
        if id in ids:
            path = PGPATH + k
            paths.append(path)
    return paths

def get_id(key):
    """ Returns an id from an object path/key. 
    Due to there being duplicates in the set (suffixed by -X.txt),
    we need to do some pattern matching to get the id """
    id = re.search(r'\d*', key.split("/")[-1]).group()
    return int(id)

if __name__ == "__main__":
    """ Count gender pronouns in all books in gutenberg catalogue """
    # create a spark context
    sc = SparkContext(appName="GenderPronounCount")
    # read catalogue index from json formatted file
    rdd = sc.textFile(PGCAT).filter(lambda x: x!= 'None')
    # create an spark session (entry point to dataframe API)
    spark = SparkSession(sc)
    # create dataframe from rdd of JSON formatted elements
    df1 = spark.read.json(rdd)
    # register dataframe as SQL temporary view
    df1.createOrReplaceTempView("pgcat")
    # query table and put results in dataframe
    df2 = spark.sql("SELECT DISTINCT id, title, author FROM pgcat WHERE lang='en'")
    # get the book ids in a separate list
    ids = df2.select("id").rdd.flatMap(lambda x: x).collect()
    # get the object keys from the s3 bucket
    keys = get_keys(BUCKET)
    # get full paths to objects, filtering by id
    paths = get_paths(ids, keys)
    # get the whole text files associated with the paths and count gender pronouns
    results = sc.wholeTextFiles(",".join(paths)).map( lambda x: cnt_pronouns( get_id(x[0]),  x[1] ) )
    # create dataframe from results rdd
    df3 = spark.createDataFrame(results,schema=['id','mcount','fcount'])
    # create dataframe by joining dataframes df2 and df3 on id field
    df4 = df2.join(df3, df2.id == df3.id, 'inner').drop(df3.id)
    # save df4 as csv formatted file, removing line breaks and carriage returns
    # NOTE: headers will not be saved. Look at df.write.csv as one of a few possible alternatives
    df4.rdd.map(lambda x: ','.join(['"'+ re.sub(r'[\r\n]+'," ", str(i)) +'"' for i in x])).repartition(1).saveAsTextFile(WORKINGDIR + 'genderPronounCount.csv')
    sc.stop()

