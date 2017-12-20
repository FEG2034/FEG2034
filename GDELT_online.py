#python 2.7
import datetime # <- built-in function
timestamp_main = datetime.datetime.now()

# Part(1) input command: spark-submit /file/path YYYYMMDD YYYYMMDD event-number---------------------
timestamp_Part1 = datetime.datetime.now()

import sys
search_range = [int(sys.argv[1]), int(sys.argv[2])] # as input(">>>Analysis period (YYYYMMDD):") in python
search_event = int(sys.argv[3]) # as input(">>>Analysis event:") in python

timecost_Part1 = (datetime.datetime.now() - timestamp_Part1).total_seconds()

# Part(2) Query the data from project-id: GDELT to our own BigQuery Project-id.Dataset.Table--------
timestamp_Part2 = datetime.datetime.now()

from google.cloud import bigquery # <- pip install
from google.cloud.bigquery import job

#source--https://cloud.google.com/bigquery/docs/python-client-migration?hl=zh-tw
bq = bigquery.Client() #project = gdelt-pyspark
gdelt_dataset = bq.dataset("DS_GDELT_dataset")

# create now table with timestamp under project-id:dataset
table_name = "result_" + str(timestamp_Part2.year)+str(timestamp_Part2.month)+str(timestamp_Part2.day)+str(timestamp_Part2.hour)+str(timestamp_Part2.minute)+str(timestamp_Part2.second)

SCHEMA = [ bigquery.SchemaField("Actor1CountryCode", "string"), bigquery.SchemaField("Actor2CountryCode", "string") ]

table_ref = gdelt_dataset.table(table_name)

table = bigquery.Table(table_ref, schema=SCHEMA)
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

import ast # <- built-in function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

conf = SparkConf()
sc = SparkContext(conf = conf) #Cannot run multiple SparkContexts at once; existing SparkContext(app=PySparkShell, master=yarn) created by <module>
SQLCtx = SQLContext(sc)

project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
input_directory = 'gs://{}/pyspark_input'.format(bucket)

conf_BQ = {
    'mapred.bq.project.id': project,
    'mapred.bq.gcs.bucket': bucket,
    'mapred.bq.temp.gcs.path': input_directory,
    'mapred.bq.input.project.id': project,
    'mapred.bq.input.dataset.id': 'DS_GDELT_dataset',
    'mapred.bq.input.table.id': table_name, # no NULL in this table
}

table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=conf_BQ) #Json is the problem, we need table/csv inputformat (use json.load)

table_values = table_data.values().map(lambda x: ast.literal_eval(x)) # catch the value of pairRDD, than convert unicode object into dist
input_list = table_values.collect()

input_DataFrame = SQLCtx.createDataFrame(input_list) # convert [dict(Actor1CountryCode, Actor2CountryCode)] into SQL.DataFrame
input_rdd = input_DataFrame.rdd.partitionBy(5)

timecost_Part3 = (datetime.datetime.now() - timestamp_Part3).total_seconds()

# Part(4) running pagerank with networkx------------------------------------------------------------
timestamp_Part4 = datetime.datetime.now()

import networkx # <- sudo pip install networkx
import pandas # <- pip install
from collections import Counter # <- built-in function

CountryCode = pandas.read_csv("https://www.gdeltproject.org/data/lookups/CAMEO.country.txt", sep='\t', lineterminator='\n')
CountryCode = list(CountryCode["CODE"])

G = networkx.DiGraph()
G.add_nodes_from(CountryCode)

def PR(edge):
    G_sub = networkx.create_empty_copy(G)
    G_sub.add_edges_from(list(edge))
    return list(networkx.pagerank(G_sub).items()) # can't add max_iter

Pagerank = input_rdd.mapPartitions(lambda x: PR(x))
Pagerank_result = Pagerank.reduceByKey(lambda x,y: x+y).collect()

ShowOff = SQLCtx.createDataFrame(Pagerank_result).toDF("Country", "Pagerank").orderBy(["Pagerank", "Country"])

timecost_Part4 = (datetime.datetime.now() - timestamp_Part4).total_seconds()

# Part(5) clean up the direstory and data in cloud storage------------------------------------------

input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)

# Final show off result in DateFrame and time cost--------------------------------------------------
print "Time cost: {} (seconds) for BigQuery (from GDELT project to our own project)".format(str(timecost_Part2))
print "Time cost: {} (seconds) for extracting data from BigQuery to Cloud Storage".format(str(timecost_Part3))
print "Time cost: {} (seconds) for running pagerank with networkx (5 partition)".format(str(timecost_Part4))
print "Time cost: {} (seconds) for the whole process".format(str((datetime.datetime.now()-timestamp_main)))
ShowOff.show(n=10)
