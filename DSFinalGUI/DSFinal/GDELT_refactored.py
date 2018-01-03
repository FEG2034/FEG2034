#Built-in imports
import ast
import sys
import datetime
import operator

#Google cloud imports
from google.cloud import bigquery
from google.cloud.bigquery import job

#PySpark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row

#NetworkX
import networkx as nx

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
	AND Actor1CountryCode IS NOT NULL AND Actor2CountryCode IS NOT NULL and Actor1Name IS NOT NULL and Actor2Name IS NOT NULL".format(GDELT_EVENT_TABLE_NAME, eventType, dateTimeFrom, dateTimeTo)
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

def preprocessing(data):
	SQLCtx = SQLContext(SPARKCONTEXT)
	dataValues = [row.values() for row in data]
	dataRDD = SPARKCONTEXT.parallelize(dataValues)
	actorDataRows = dataRDD.map(lambda actor : Row(Actor1CountryCode=actor[0], Actor2CountryCode=actor[1]))
	inputDataFrame = SQLCtx.createDataFrame(actorDataRows) 
	queryResult = inputDataFrame.select("Actor1CountryCode", "Actor2CountryCode")
	relationships = queryResult.rdd.map(lambda data : (data.Actor1CountryCode, data.Actor2CountryCode)).collect()#By document of networkx, nodes will automatically be added when adding an edge so only relationships matter
	return relationships

def createGraphFromRelationships(edges):
	G = nx.DiGraph()
	for edge in edges:
		if G.has_edge(edge[0], edge[1]):
			G[edge[0]][edge[1]]["weight"] += 1
		else:
			G.add_edge(edge[0], edge[1], weight=1)
	"""
	for edge in G.edges:
		print("Edge : {0}-->{1}, Weight : {2}".format(edge[0], edge[1], G[edge[0]][edge[1]]["weight"]))
	print("================")
	"""
	return G

def calculatePageRank(graph):
	return nx.pagerank_scipy(graph, tol=0.0001)

def main():
	eventType, dateTimeFrom, dateTimeTo = getArgs()
	queryResult = queryGDELT(eventType, dateTimeFrom, dateTimeTo)

	relationships = preprocessing(queryResult)

	graph = createGraphFromRelationships(relationships)
	pageRank = sorted(calculatePageRank(graph).items(), key=operator.itemgetter(1), reverse=True)
	
	for result in pageRank[:10]:
		print("{0} : {1}".format(result[0], result[1]))
	
	SPARKCONTEXT.stop()
	return

main()

