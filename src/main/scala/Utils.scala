import org.apache.spark.rdd
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer

/**
 * alvaro, @alvaromuir 
 * created on 1/18/15.
 */

object Utils {
  /** returns a List[String] of serialized RDD row. Specific to DFA data lines with double quotes on some of the
    * site_dfa (publisher) or placement names
    * @param line is the RDD line you want to clean
    *
    */
  def SerDe(line: String) =  {
    val result = new ArrayBuffer[String]()

    if (line.contains("\"")) {
      val dirty = line.toString.replace(",\"",",^\"").
        replace("\",","\"^,").
        split('^')

      for(i <- dirty)
        if (i.contains("\"")) {
          result += i.replace("\"","")
        }
        else {
          for (x <- i.split(","))
            result += x
        }
    }
    else {
      for (x <- line.toString.split(",")) result += x
    }
    result.filter(! _.toString.isEmpty).toList
  }



  /** returns a List[String] of specific placement by index in RDD
    * @param data the rdd you wish to search
    */
  def getLine(num: Int, data: rdd.RDD[String]) = {
    val indexedRdd = data.zipWithIndex().map { case (k,v) => (v,k) }
    indexedRdd.lookup(num -1)
  }



  /** returns Int as index of specific column name from a List[String]
    * @param data the rdd you wish to search
    */
  def columnIndex(col: String, data: List[String]): Int = {
    data.indexOf(col)
  }



  /** returns a List of unique elements of a specific 'column'. If the 'column' is
    * not found, returns null
    * @param col the 'column' you want to sort
    * @param fieldList the 'fields' available to rdd as a List[String]
    * @param data the rdd you wish to sort
    * ex:
    *    distinctColumn("site_dfa", fieldList, data)
    */
  def distinctColumn(col: String, fieldList: List[String], data: rdd.RDD[String]): List[String] = {
    if (fieldList.indexOf(col) > -1)
      data.map(x => SerDe(x)(columnIndex(col,fieldList))).collect().distinct.toList
    else null
  }



  /** returns a RDD String of valid data for a specified RDD
    * @param index string to search
    * @param srcList list of fields you want to check against
    * @param fieldList the 'fields' available to rdd as a List[String]
    * @param data the rdd you wish to sort
    */
  def validData(index: String, srcList: List[String], fieldList: List[String], data: rdd.RDD[String]): rdd.RDD[String] =
    data.filter(line => srcList contains SerDe(line)(columnIndex(index, fieldList)))



  /** returns a RDD String of invalid data for a specified RDD
    * @param index string to search
    * @param srcList list of fields you want to check against
    * @param fieldList the 'fields' available to rdd as a List[String]
    * @param data the rdd you wish to sort
    */
  def invalidData(index: String, srcList: List[String], fieldList: List[String], data: rdd.RDD[String]): rdd.RDD[String] =
    data.filter(line => !(srcList contains SerDe(line)(columnIndex(index, fieldList))))



  /** map/reduces total number for a specific 'column'
    * @param col the rdd you wish to add
    * @param fieldList the 'fields' available to rdd as a List[String]
    * @param data the rdd you wish to sort
    */

  def aggregateColumn(col: String, fieldList: List[String], data: rdd.RDD[String]) =
    data.map(x =>SerDe(x)(columnIndex(col, fieldList)).toInt).
      collect().reduce(_+_)



  /** returns a List of String RDD's grouped by selected rdd 'column', e.g. "campaign"
    * or "site_dfa". If 'column' does not exist, returns null.
    * @param col the 'column' you want to sort by
    * @param data the rdd you wish to sort. dataDfa is the default
    */
  def dataBy(col: String, fieldList: List[String], data: rdd.RDD[String]): List[rdd.RDD[String]] = {
    val qualifier = distinctColumn(col, fieldList, data)
    if (qualifier != null) {
      val result = {
        for (q <- qualifier) yield {
          val filtered =
            data.filter(x => SerDe(x)(columnIndex(col, fieldList)) == q)
          for (line <- filtered) yield line
        }
      }
      result
    }
    else null
  }



  /** returns a RDD String of out of desired publisher for a specified RDD
    * @param pub the publisher you want to search for
    * @param data the rdd you wish to search. dataDfa is the default
    */
  def getPublisher(pub: String, fieldList: List[String], data: rdd.RDD[String]): rdd.RDD[String] =
    data.filter(x => SerDe(x)(columnIndex("site_dfa", fieldList)) == pub)



  /** returns a RDD String of out of desired buying model for a specified RDD
    * @param model the buy model you want to search for
    * @param data the rdd you wish to search. dataDfa is the default
    */
  def getBuyModel(model: String, fieldList: List[String], data: rdd.RDD[String]): rdd.RDD[String] =
    data.filter(x => SerDe(x)(columnIndex("placement_cost_structure", fieldList)) == model)

  /** genrates a snapshot report of specified data set
    * e.g.:
    *       imps: Total: 12345       	in: 11111     	out: 1234     	% out: 0.01
    *     clicks: Total: 100          in: 95        	out: 5        	% out: 0.05
    * @param data the data set you wish to report on. dataDFA is the default
    */

  def marketReport(marketList: List[String], fieldList: List[String], data: rdd.RDD[String]) = {
    val dataIn            = validData("designated_market_area_dma", marketList, fieldList, data)
    val dataOut           = invalidData("designated_market_area_dma", marketList, fieldList, data)
    val inImps: Int       = aggregateColumn("impressions", fieldList, dataIn)
    val outImps: Int      = aggregateColumn("impressions", fieldList, dataOut)
    val ttlImps: Int      = inImps + outImps
    val pctOutImps: Float = outImps/ttlImps.toFloat

    val inClicks: Int     = aggregateColumn("clicks", fieldList, dataIn)
    val outClicks: Int    = aggregateColumn("clicks", fieldList, dataOut)
    val ttlClicks: Int    = inClicks + outClicks
    val pctOutClicks: Float = outClicks/ttlClicks.toFloat

    print(f"  imps: Total: ${ ttlImps.toString.padTo(11, " ").mkString } \t" +
      f"in: ${ inImps.toString.padTo(8, " ").mkString }  \t" +
      f"out: ${ outImps.toString.padTo(8, " ").mkString } \t" +
      f"%% out: ${ pctOutImps }%.2f")

    print("\n")

    print(f"clicks: Total: ${ ttlClicks.toString.padTo(11, " ").mkString } \t" +
      f"in: ${ inClicks.toString.padTo(8, " ").mkString }  \t" +
      f"out: ${ outClicks.toString.padTo(8, " ").mkString } \t" +
      f"%% out: ${ pctOutClicks }%.2f")
  }

}