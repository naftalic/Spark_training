# Some important notes about this resource

The pgcat.py script is an example solution to the week 3 peer assessment task. 
It has been tested in Spark 2.0 (local and cluster mode), and is thought to be compatible with Spark 1.6.

1. The script assumes you are using Amazon cloud storage (S3) to store 
our subset of the Gutenberg data. If you are not, you will need to edit
parts of the script which utilise the boto3 toolkit.

2. You will need to set the s3n properties of core-site.xml on master node (each time you launch/start cluster):

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

3. You will need to set PYSPARK_PYTHON on all nodes (each time you launch/start cluster):

		echo "PYSPARK_PYTHON=/usr/bin/python35" > conf/spark-env.sh
		pssh -h conf/slaves echo "PYSPARK_PYTHON=/usr/bin/python35" > spark/conf/spark-env.sh

(NB: The second command is not required if you set --copy-aws-credentials)

4. You will need to set up a default profile in ./aws/credentials on the master node (one-time-only configuration):

		[default]
		aws_access_key_id = XXXXXXXXXXXXXXXXXXXX
		aws_secret_access_key = XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

5. The results generated will be in the format provided in pgcat.json (note the file has been truncated at 10 entries)
