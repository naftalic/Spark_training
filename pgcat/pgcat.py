from rdflib import Graph, Literal
from rdflib.namespace import Namespace, RDF, DCTERMS
from rdflib.term import bind

from pyspark import SparkContext

import re, boto3, json

PGTERMS = Namespace(u'http://www.gutenberg.org/2009/pgterms/')
BUCKET = 'YOUR_BUCKET' # name of the S3 bucket where your data resides
PGDIR = 's3n://' + BUCKET + '/gutenberg_dataset/' # path on s3 where data resides
WORKINGDIR = 's3n://' + BUCKET + '/results/' # path on s3 where you'll save your results
bind(DCTERMS.RFC4646, str)

def get_tuple(content):
    """
    Get select fields from a meta file by id
    Return a tuple in format: (book, author, title, language)
    """
    g = Graph()
    g.parse(data=content, format='application/rdf+xml')
    qres = g.query(
    """
    PREFIX pgterms: <http://www.gutenberg.org/2009/pgterms/>
    SELECT DISTINCT ?book ?author ?title ?language WHERE {
        ?book a pgterms:ebook .
        ?book dcterms:language ?lang .
        ?lang rdf:value ?language .
        ?book dcterms:title ?title .
        ?book dcterms:creator ?agent .
        ?agent pgterms:name ?author .
    }
    """)
    return [row for row in qres]

def get_row(id, content):
    """ Format a row from a tuple as json object """
    tup = get_tuple(re.sub(r'[\r\n]+',' ', content))
    keys = ['id', 'book', 'author', 'title', 'lang']
    if tup:
        values = [id] + [ x.toPython() for x in tup[0] ]
        row = json.dumps(dict(zip(keys, values)))
        return row

def get_bookid(filename):
    """ Extract book id from the name of the rdf file """
    id = re.sub('.rdf$', '', filename.split('/pg',1)[1])
    return int(id)

if __name__ == "__main__":
    """
    Create lists from gp meta in format [id, book, author, title, lang]
    Save to Amazon S3 as json format file
    """
    # specify the profile to use
    session = boto3.Session(profile_name='default')
    # create a client object
    s3_client = session.client('s3')
    # retrieve list of buckets
    response = s3_client.list_buckets()
	# put bucket names in a list
    buckets = [x.get('Name') for x in response.get('Buckets')]
    # if BUCKET is in list of buckets
    if BUCKET in buckets:
        print('Bucket exists')
        # create a spark context
        sc = SparkContext(appName="GenPGCat")
        # make an RDD from the meta files
        rdd = sc.wholeTextFiles(PGDIR + 'meta/*').filter( lambda x: re.search('.rdf$', x[0]))
        # get the formated rows for writing to a json compatible file
        rows = rdd.map( lambda x: get_row( get_bookid(x[0]), x[1]) )
        #repartition the rdd and save as single file
        rows.repartition(1).saveAsTextFile(WORKINGDIR + "pgcat-cluster.json")

        # shut down the spark context
        sc.stop()
    else:
        print("bucket doesn't exist")
