# env: python 2.7

# built-in package
import datetime
import sys
import ast
from collections import Counter

# NetworkX
import networkx as nx

# google-cloud package <- sudo pip install google-cloud
from google.cloud import bigquery
from google.cloud.bigquery import job

#PySpark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

timestamp_main = datetime.datetime.now()

# Part(1) input command: spark-submit /file/path YYYYMMDD YYYYMMDD event-number---------------------
search_range = [int(sys.argv[1]), int(sys.argv[2])]
search_event = int(sys.argv[3])

# Part(2) Query the data from GDELT to our own BigQuery Project-id.Dataset.Table--------------------
timestamp_Part2 = datetime.datetime.now()

DATASET_BigQuery = "DS_final"
TABLE_BigQuery = "GDELT_" + str(search_event) + "_" + timestamp_Part2.strftime("%Y%m%d%H%M%S")
SCHEMA_BigQuery = [ bigquery.SchemaField("Actor1CountryCode", "string"), bigquery.SchemaField("Actor2CountryCode", "string") ]

bq = bigquery.Client()
bq_dataset = bq.dataset(DATASET_BigQuery)

table_ref = bq_dataset.table(TABLE_BigQuery)
table = bigquery.Table(table_ref, schema=SCHEMA_BigQuery)
table = bq.create_table(table)

# running query
QUERY = "SELECT Actor1CountryCode, Actor2CountryCode FROM `gdelt-bq.gdeltv2.events` \
    WHERE EventCode LIKE '{0}%' and SQLDATE >= {1} and SQLDATE <= {2} \
    AND Actor1CountryCode IS NOT NULL AND Actor2CountryCode IS NOT NULL".format(search_event, search_range[0], search_range[1])

job_config = bigquery.QueryJobConfig()
job_config.destination = table_ref
query_job = bq.query(QUERY, job_config=job_config)

iterator = query_job.result()

timecost_Part2 = (datetime.datetime.now() - timestamp_Part2).total_seconds()

# Part(3) Load data from BigQuery to cloud storage--------------------------------------------------
timestamp_Part3 = datetime.datetime.now()

conf = SparkConf()
sc = SparkContext(conf = conf)
SQLCtx = SQLContext(sc)

project_GCP = "testbigqueryforgdelt"#sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
bucket_GCP = "dataproc-9ca6db64-3885-4b1b-9169-e96fc1553b9a-asia-east1"#sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
input_directory = 'gs://{}/pyspark_input'.format(bucket_GCP)

conf_BQ = {
    'mapred.bq.project.id': project_GCP,
    'mapred.bq.gcs.bucket': bucket_GCP,
    'mapred.bq.temp.gcs.path': input_directory,
    'mapred.bq.input.project.id': project_GCP,
    'mapred.bq.input.dataset.id': DATASET_BigQuery,
    'mapred.bq.input.table.id': TABLE_BigQuery, # no NULL in this table
}

table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=conf_BQ)

# catch the value of pairRDD, than convert unicode object into dist
table_values = table_data.values().map(lambda x: ast.literal_eval(x)) 

# convert [dict(Actor1CountryCode, Actor2CountryCode)] into SQL.DataFrame
input_DataFrame = SQLCtx.createDataFrame(table_values) 

partition_number = 5
input_rdd = input_DataFrame.rdd.partitionBy(partition_number)

timecost_Part3 = (datetime.datetime.now() - timestamp_Part3).total_seconds()

# Part(4) running pagerank with networkx------------------------------------------------------------
timestamp_Part4 = datetime.datetime.now()

def PR(edge):
    G = nx.DiGraph()
    G.add_edges_from(list(edge))
    return list(nx.pagerank(G).items()) # can't add max_iter

Pagerank = input_rdd.mapPartitions(lambda x: PR(x))
Pagerank_result = Pagerank.reduceByKey(lambda x,y: x+y).collect()

ShowOff = SQLCtx.createDataFrame(Pagerank_result).toDF("Country", "Pagerank").orderBy(["Pagerank", "Country"])

timecost_Part4 = (datetime.datetime.now() - timestamp_Part4).total_seconds()

# Part(5) clean up the direstory and data in cloud storage------------------------------------------

input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)

# Final show off result in DateFrame and time cost--------------------------------------------------
print ("Time cost: {} (seconds) for BigQuery (from GDELT project to our own project)".format(str(timecost_Part2)))
print ("Time cost: {} (seconds) for extracting data from BigQuery to Cloud Storage".format(str(timecost_Part3)))
print ("Time cost: {} (seconds) for running pagerank with networkx (5 partition)".format(str(timecost_Part4)))
print ("Time cost: {} (seconds) for the whole process".format(str((datetime.datetime.now()-timestamp_main))))
ShowOff.show(n=10)
