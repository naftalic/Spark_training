# Some important notes about this resource

This resource is an example solution to the week 4 peer assessment.

1. There are 2 Python scripts provided, both requiring Python 3:
	gender_pronoun_v1-6.py : use with Spark 1.6
	gender_pronoun_v2.py : use with Spark 2.0+

Use whichever is relevant to your version of Spark.

2. The script depends on a solution from last week's peer assessment,
which asked that you produce a catalogue index file in json format.
In case you didn't do this, a full example solution is given in pgcat.json

3. The script assumes you are using Amazon cloud storage (S3) to store 
our subset of the Gutenberg data. If you are not, you will need to edit
parts of the script which utilise the boto3 toolkit.

4. Before running the script you should edit the global variables at the top

5. If reading/writing to s3, you will need to set the s3n properties of 
core-site.xml on master node (each time you launch/start cluster):

		<property>
		  <name>fs.s3n.awsAccessKeyId</name>
		  <description>AWS access key ID</description>
		  <value>AKIAJXB4_EXAMPLE_ACCESS_KEY_ID</value>
		</property>

		<property>
		  <name>fs.s3n.awsSecretAccessKey</name>
		  <description>AWS secret key</description>
		  <value>f2zf6Irq6t+X3fvLmR_EXAMPLE_SECRET_KEY</value>
		</property>

5. You will need to set PYSPARK_PYTHON on all nodes (each time you launch/start cluster):

		echo "PYSPARK_PYTHON=/usr/bin/python35" > conf/spark-env.sh
		pssh -h conf/slaves echo "PYSPARK_PYTHON=/usr/bin/python35" > spark/conf/spark-env.sh

(NB: The second command is not required if you set --copy-aws-credentials)

6. If using S3 (and you have not done so already), you will need to set up a default profile 
in ./aws/credentials on the master node (one-time-only configuration):

		[default]
		aws_access_key_id = XXXXXXXXXXXXXXXXXXXX
		aws_secret_access_key = XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

7. The results generated are in csv format, WITHOUT headers. The headers are: 
	[ "id", "title", "author", "male pronoun count", "female pronoun count" ]

An example (truncated) at 10 entries has been provided in genderPronoun.csv
