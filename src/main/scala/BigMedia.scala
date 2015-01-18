import org.apache.spark.{SparkConf, SparkContext, rdd}
import Utils.{ columnIndex, distinctColumn, marketReport }

/**
 * alvaro, @alvaromuir 
 * created on 1/18/15.
 */

object BigMedia {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BigMedia")
    val sc = new SparkContext(conf)


    val marketsHeaders: List[String] = sc.textFile("2015/dfa/market_fields.csv").first().split(",").toList


    val marketsData: rdd.RDD[String] = sc.textFile("2015/dfa/markets.csv")

    val dfaHeaders: List[String] =
      sc.textFile("2015/dfa/fields.csv").first().split(",").toList

    val dfaData: rdd.RDD[String] =
      sc.textFile("2015/dfa/*sample.csv").cache()


    val serviceRegions: List[String] =
      marketsData.map(m => m.split(",")(columnIndex("Region", marketsHeaders))).
        collect().distinct.toList

    val serviceStates: List[String] =
      marketsData.map(m => m.split(",")(columnIndex("State", marketsHeaders))).
        collect().distinct.filter(! _.contains("MD/VA")).toList

    val serviceDMAS: List[String] = marketsData.map(m => {
      val line = m.split(",")
      val length = line.length
      if (length == 7) line(4) else line(3)
    }).
      collect().distinct.filter(! _.contains("DC/MD/VA")).toList


    val validDMAS: List[String] =
      distinctColumn("designated_market_area_dma", dfaHeaders, dfaData).filter(serviceDMAS.contains(_))

    val invalidDMAS: List[String] =
      distinctColumn("designated_market_area_dma", dfaHeaders, dfaData).filterNot(serviceDMAS.contains(_))

    marketReport(serviceDMAS, dfaHeaders, dfaData)

  }
}