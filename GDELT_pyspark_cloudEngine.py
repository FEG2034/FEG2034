#python 2.7
import networkx # <- sudo pip install networkx
import pandas # <- pip install
from collections import Counter # <- built-in function

from pyspark import SparkConf, SparkContext
conf = SparkConf()
sc = SparkContext(conf = conf)

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
SQLCtx = SQLContext(sc)

import sys
search_range = [int(sys.argv[1]), int(sys.argv[2])] # as input(">>>Analysis period (YYYYMMDD):") in python
search_event = int(sys.argv[3]) # as input(">>>Analysis event:") in python

#Load data from bigquery to storage--------------------------------------------------
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
input_directory = 'gs://{}/pyspark_input'.format(bucket)

conf_BQ = {
    'mapred.bq.project.id': project,
    'mapred.bq.gcs.bucket': bucket,
    'mapred.bq.temp.gcs.path': input_directory,
    #'mapred.bq.input.project.id': 'gdelt-bq',
    #'mapred.bq.input.dataset.id': 'gdeltv2',
    #'mapred.bq.input.table.id': 'events',
    'mapred.bq.input.project.id': project,
    'mapred.bq.input.dataset.id': 'DS_GDELT_dataset',
    'mapred.bq.input.table.id': 'results_20171206_185901',
    #'mapred.bq.input.query': "SELECT Actor1CountryCode, Actor2CountryCode FROM `gdelt-bq.gdeltv2.events` WHERE EventCode LIKE '{0}%' and SQLDATE >= {1} and SQLDATE <= {2}".format(search_event, search_range[0], search_range[1])
}

#--Load data in from BigQuery.
table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=conf_BQ) #Json is the problem, we need table/csv inputformat

#---------------------------------------------------------------------

CountryCode = pandas.read_csv("https://www.gdeltproject.org/data/lookups/CAMEO.country.txt", sep='\t', lineterminator='\n')
CountryCode = list(CountryCode["CODE"])

input = SQLCtx.createDataFrame(table_data).dropna()
input_rdd = input.rdd.partitionBy(5)

#pagerank with networkx-----------------------------------------------
G = networkx.DiGraph()
G.add_nodes_from(CountryCode)

def PR(edge):
    G_sub = networkx.create_empty_copy(G)
    G_sub.add_edges_from(list(edge))
    return list(networkx.pagerank(G_sub).items()) # can't add max_iter

Pagerank = input_rdd.mapPartitions(lambda x: PR(x))
Pagerank_result = Pagerank.reduceByKey(lambda x,y: x+y).collect()

#ShowOff = SQLCtx.createDataFrame(Pagerank_result).toDF("Country", "Pagerank").orderBy(["Pagerank", "Country"])
#ShowOff.show(n=10)
