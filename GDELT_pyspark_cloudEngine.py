from google.cloud import bigquery # <- pip install
import networkx # <- pip install
import pandas # <- pip install
from collections import Counter # <- built-in function

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("GDELT")
sc = SparkContext(conf = conf)

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
spark_SQL = SparkSession(sc)

#search_range = input(">>>Analysis period (YYYYMMDD):")
#search_event = input(">>>Analysis event:")
search_range = [20170303, 20170303] 
search_event = 19 

#BigQuery on python---------------------------------------------------
def GDELT_BigQuery(search_range, search_event):
    QUERY_W_PARAM = "SELECT Actor1CountryCode, Actor2CountryCode FROM [gdelt-bq:gdeltv2.events] \
        WHERE EventCode LIKE '{0}%' and SQLDATE >= {1} and SQLDATE <= {2}" \
        .format(int(search_event), search_range[0], search_range[1])

    client = bigquery.Client()
    TIMEOUT = 30  # in seconds
    param = bigquery.ScalarQueryParameter('state', 'STRING', 'TX')
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = [param]
    query_job = client.query(QUERY_W_PARAM, job_config=job_config)

    return query_job.to_dataframe()
#---------------------------------------------------------------------
#BigQuery on pyspark--------------------------------------------------
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_input'.format(bucket)

conf_BQ = {
    'mapred.bq.project.id': project,
    'mapred.bq.gcs.bucket': bucket,
    'mapred.bq.temp.gcs.path': input_directory,
    'mapred.bq.input.project.id': 'gdelt-bq',
    'mapred.bq.input.dataset.id': 'gdeltv2',
    'mapred.bq.input.table.id': 'events'
    'mapred.bq.input.query': 'SELECT Actor1CountryCode, Actor2CountryCode FROM [gdelt-bq:gdeltv2.events] \
        WHERE EventCode LIKE '{0}%' and SQLDATE >= {1} and SQLDATE <= {2}'.format(int(search_event), search_range[0], search_range[1])
}

conf_BQ_test = {
    'mapred.bq.input.table.id': 'gdelt-bq:gdeltv2.events.2017',
}

#--Load data in from BigQuery.
table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=conf_BQ_test)

#---------------------------------------------------------------------

#load data
CountryCode = pandas.read_csv("https://www.gdeltproject.org/data/lookups/CAMEO.country.txt", sep='\t', lineterminator='\n')
CountryCode = list(CountryCode["CODE"])

#BigQuery replace here------------------------------------------------------------------------------------------------------------------------------------------------
input = spark_SQL.read.csv("file:///Users/feg2034/GDELT_temp/GDELT - 20150218230000.export.CSV", header=True).select("Actor1CountryCode", "Actor2CountryCode") #-> return SQL.DataFrame with header
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------

input = input.fillna("NONE")
input_rdd = input.rdd

#construct graph by NetWorkX
G = networkx.DiGraph()
G.add_nodes_from(CountryCode)

def PR(edge):
    G_sub = networkx.create_empty_copy(G)
    G_sub.add_edge(edge[0], edge[1])
    return list(networkx.pagerank(G_sub).items()) # can't add max_iter

Pagerank = input_rdd.map(lambda x: PR(tuple(x)))
Pagerank_result = Pagerank.reduce(lambda x,y: Counter(x)+Counter(y))

ShowOff = spark_SQL.createDataFrame(Pagerank_result).toDF("Country", "Pagerank").orderBy(["Pagerank", "Country"])
ShowOff.show(n=10)
