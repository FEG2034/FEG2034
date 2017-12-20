#python 2.7

# Part(1) input command: spark-submit /file/path YYYYMMDD YYYYMMDD event-number---------------------
import sys
search_range = [int(sys.argv[1]), int(sys.argv[2])] # as input(">>>Analysis period (YYYYMMDD):") in python
search_event = int(sys.argv[3]) # as input(">>>Analysis event:") in python

# Part(2) Load data from BigQuery to cloud storage--------------------------------------------------

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
    #'mapred.bq.input.project.id': 'gdelt-bq',
    #'mapred.bq.input.dataset.id': 'gdeltv2',
    #'mapred.bq.input.table.id': 'events',
    'mapred.bq.input.project.id': project,
    'mapred.bq.input.dataset.id': 'DS_GDELT_dataset',
    'mapred.bq.input.table.id': 'results_20171220_200900', # no NULL in this table
    #'mapred.bq.input.query': "SELECT Actor1CountryCode, Actor2CountryCode FROM `gdelt-bq.gdeltv2.events` WHERE EventCode LIKE '{0}%' and SQLDATE >= {1} and SQLDATE <= {2}".format(search_event, search_range[0], search_range[1])
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

# Part(3) running pagerank with networkx------------------------------------------------------------

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

#ShowOff = SQLCtx.createDataFrame(Pagerank_result).toDF("Country", "Pagerank").orderBy(["Pagerank", "Country"])
#ShowOff.show(n=10)

# Part(4) clean up the direstory and data in cloud storage------------------------------------------
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
