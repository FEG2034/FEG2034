#Built-in imports
import ast
import sys
import datetime

#Google cloud imports
from google.cloud import bigquery
from google.cloud.bigquery import job

#PySpark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row

#NetworkX
import networkx

#Global variables
GDELT_EVENT_TABLE_NAME = "`gdelt-bq.gdeltv2.events`"
DATASET_NAME_ON_BIGQUERY = "DS_final"
TABLE_NAME_IN_DATASET = "Gdeltv2EventFiltered_" + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
RDD_PARTITIONS = 5
SCHEMA = [bigquery.SchemaField("Actor1CountryCode", "string"), bigquery.SchemaField("Actor2CountryCode", "string")]
SPARK_CONF = SparkConf()
SPARKCONTEXT = SparkContext(conf = SPARK_CONF)
SQLCtx = SQLContext(SPARKCONTEXT)

def getArgs():
	dateTimeFrom = int(sys.argv[1])
	dateTimeTo = int(sys.argv[2])
	eventType = int(sys.argv[3])
	return eventType, dateTimeFrom, dateTimeTo

def createBigQueryTableBySchema(bigQueryClient, schema):
	dataset = bigQueryClient.dataset(DATASET_NAME_ON_BIGQUERY)
	tableRef = dataset.table(TABLE_NAME_IN_DATASET)

	newTable = bigquery.Table(tableRef, schema=schema)
	bigQueryClient.create_table(newTable)

	return tableRef

def createQueryCommand(eventType, dateTimeFrom, dateTimeTo):
	queryCmd = "SELECT Actor1CountryCode, Actor2CountryCode FROM {0} \
	WHERE EventCode LIKE '{1}%' and SQLDATE >= {2} and SQLDATE <= {3} \
	AND Actor1CountryCode IS NOT NULL AND Actor2CountryCode IS NOT NULL LIMIT 100".format(GDELT_EVENT_TABLE_NAME, eventType, dateTimeFrom, dateTimeTo)
	return queryCmd

def queryGDELT(eventType, dateTimeFrom, dateTimeTo):
	bigQueryClient = bigquery.Client()

	queryCmd = createQueryCommand(eventType, dateTimeFrom, dateTimeTo)
	tableRef = createBigQueryTableBySchema(bigQueryClient, SCHEMA)

	jobConfig = bigquery.QueryJobConfig()
	jobConfig.allow_large_results = True
	jobConfig.destination = tableRef
	queryJob = bigQueryClient.query(queryCmd, job_config=jobConfig)

	iterator = queryJob.result()
	return list(iterator)

def getDataFromBigQuery():
	project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
	bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
	input_directory = 'gs://{}/pyspark_input'.format(bucket)

	confBQ = {
		'mapred.bq.project.id': project,
		'mapred.bq.gcs.bucket': bucket,
		'mapred.bq.temp.gcs.path': input_directory,
		'mapred.bq.input.project.id': project,
		'mapred.bq.input.dataset.id': DATASET_NAME_ON_BIGQUERY,
		'mapred.bq.input.table.id': TABLE_NAME_IN_DATASET, # no NULL in this table
	}

	tableData = sc.newAPIHadoopRDD(
		'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
		'org.apache.hadoop.io.LongWritable',
		'com.google.gson.JsonObject',
		conf=confBQ)

	return tableData

def preprocessing(data):
	SQLCtx = SQLContext(SPARKCONTEXT)
	dataValues = [row.values() for row in data]
	dataRDD = SPARKCONTEXT.parallelize(dataValues)
	actors = dataRDD.map(lambda actor : Row(Actor1CountryCode=actor[0], Actor2CountryCode=actor[1]))
	#tableValues = data.values().map(lambda x: ast.literal_eval(x)) # catch the value of pairRDD, than convert unicode object into dist
	inputDataFrame = SQLCtx.createDataFrame(actors) # convert [dict(Actor1CountryCode, Actor2CountryCode)] into SQL.DataFrame
	inputRDD = inputDataFrame.rdd.partitionBy(RDD_PARTITIONS)
	for data in inputRDD.collect():
		print data
	return inputRDD

def createGraphFromDataFrame(data):
	graph = networkx.DiGraph()
	#TODO
	return graph

def calculatePageRank(graph):
	#TODO
	return 0.0

def main():
	eventType, dateTimeFrom, dateTimeTo = getArgs()
	queryResult = queryGDELT(eventType, dateTimeFrom, dateTimeTo)
	for result in queryResult:
		print result
		print result[0]
		print result[1]
	"""
		print "========================="
		rawData.append((result[0], result[1]))
	print rawData
	"""
	#testRDD = preprocessing(queryResult)
	dataRDD = preprocessing(queryResult)

	graph = createGraphFromDataFrame(dataRDD)
	pageRank = calculatePageRank(graph)

	print pageRank
	return

main()

