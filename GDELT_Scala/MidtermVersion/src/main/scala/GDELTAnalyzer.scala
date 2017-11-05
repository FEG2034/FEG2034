import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import scala.util.hashing.MurmurHash3

object GDELTAnalyzer{
    def main(args:Array[String]){
        val spark = SparkSession.builder().master("local[*]").appName("Test").getOrCreate
        spark.sparkContext.setLogLevel("ERROR")

        val SQLContext = spark.sqlContext
        import SQLContext.implicits._


        val GDELTHeader = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(/*"/Users/psyman/Documents/HW/Nccucs_work/DS TODO : Filepath*/"./Header.csv").columns

        var GDELTDataFrame = spark.read.format("csv").option("header", "false").option("delimiter", "\t").load(/*"/Users/psyman/Documents/HW/Nccucs_work/DS TODO : Filepath*/"./20171023121500.export.CSV").toDF(GDELTHeader:_*)
        GDELTDataFrame = GDELTDataFrame.filter($"EventCode".startsWith("19") and $"Actor1Name".isNotNull and $"Actor2Name".isNotNull and $"Actor1CountryCode".isNotNull and $"Actor2CountryCode".isNotNull).toDF()

        val Actors = GDELTDataFrame.select("Actor1CountryCode", "Actor2CountryCode").flatMap( x => Iterable(x(0).toString, x(1).toString())).distinct()
        val Relationships = GDELTDataFrame.select("Actor1CountryCode", "Actor2CountryCode").rdd

        val ActorVertices:RDD[(VertexId, String)] = Actors.map(x => (MurmurHash3.stringHash(x).toLong, x)).rdd
        var ActionEdges = Relationships.map( x => ((MurmurHash3.stringHash(x(0).toString), MurmurHash3.stringHash(x(1).toString)), 1)).reduceByKey(_+_).map(x => Edge(x._1._1, x._1._2, x._2))

        val DangerGraph = Graph(ActorVertices, ActionEdges)

        println(s"Number of vertices: ${DangerGraph.numVertices}")
        println(s"Number of edges: ${DangerGraph.numEdges}")

        //Start measuring
        val StartTime = System.nanoTime()

        val PageRank = DangerGraph.pageRank(0.0001).vertices

        //End measuring
        val EndTime = System.nanoTime()
        println(s"Elapsed : " + (EndTime-StartTime)/1000000000)

        val DangerousRank = PageRank.join(ActorVertices).sortBy(_._2._1, ascending=false).map(_._2)
        DangerousRank.take(10).foreach(println)

        spark.stop()

    }
}
