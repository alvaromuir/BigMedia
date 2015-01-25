import Utils.{buildArgsMap, getDMAS, timer, snapshotReport}
import org.apache.spark.{SparkConf, SparkContext, rdd}
/**
 * alvaro, @alvaromuir
 * created on 1/18/15
 * updated on 1/22/15
 */

object BigMedia {



  def main(args: Array[String]) {
    val params = buildArgsMap(args)
    val dataDir = params("dataDir")
    val metaRoot = params("metaDir")

    val conf = new SparkConf().setAppName("BigMedia")
    val sc = new SparkContext(conf)


    val marketsHeaders: List[String] = sc.textFile(metaRoot + "/markets_fields.csv").first().split(",").toList
    val marketsData: rdd.RDD[String] = sc.textFile(metaRoot + "/markets.csv")
    val dfaHeaders: List[String] = sc.textFile(metaRoot + "/fields.csv").first().split(",").toList
    
    val digitalDMAS: List[String]  = getDMAS(marketsData)
    val dfaData: rdd.RDD[String] = sc.textFile(dataDir)
    
    timer(snapshotReport(digitalDMAS, dfaHeaders, dfaData).foreach(println))
  }
}
