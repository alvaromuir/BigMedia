import org.apache.spark.rdd
import org.apache.spark.SparkContext._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * alvaro, @alvaromuir 
 * created on 1/18/15.
 */

object Utils {

  /**
   * Returns a Map of params key-value pairs
   * @params is the Array of args you wish to build from
   */
  def buildArgsMap(params: Array[String]): mutable.Map[String, String] = {
    val argsMap = mutable.Map[String,String]()
    params.foreach(f = x => {
      if (x.contains("=")) {
        val param = x.split("=")
        val key = param(0).toString.replace("--", "")
        val value = param(1).toString
        argsMap.put(key, value)
      }
    })
    argsMap
  }
  
  
  /**
   * Returns time in milliseconds it took a function [A] to run
   * @param f function to run
   * @tparam A
   * @return Human-readable timestamp
   */
  
  def timer[A](f: => A): A = {
    def now = System.currentTimeMillis()
    val start = now; val a= f; val end = now
    println(s"\nExecuted in ${end-start} ms")
    a
  }
  
  
  /** Returns a List[String] of serialized RDD row. Specific to DFA data lines with double quotes on some of the
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

      for(i <- dirty) {
        if (i.contains("\"")) {
          result += i.replace(",","").replace("\"","")
        }
        else {
          for (x <- i.split(","))
            result += x
        }
      }
    }
    else {
      for (x <- line.toString.split(",")) result += x
    }
    result.filter(! _.toString.isEmpty).toList
  }


  /**
   * Returns a List[String] of a particular geo-type from a list of markets
   * @param geoType type of region you want
   * @param fieldList fields of the markets you are searching
   * @param data markets list
   * @return List[String]
   * ex: val serviceRegions: List[String] = getGeo("Region", marketsHeaders, marketsData)
   * ex: val serviceStates: List[String] = getGeo("State", marketsHeaders, marketsData).filter(! _.contains("MD/VA")).toList
   */
  def getGeo(geoType: String, fieldList: List[String], data: rdd.RDD[String]): List[String] =
    data.map(m => {
      val line = SerDe(m)
      val length = line.length
      if (length == 8) line(4) else line(3)
    }).collect().distinct.toList

  
  /**
   * Returns a List[String] DMAS from a market list. Parses markets names to return safe strings
   * @param data markets list
   * @return List[String]
   * ex: val serviceDMAS: List[String] = getDMAS(marketsData).filter(! _.contains("DC/MD/VA")).toList
   */
  def getDMAS(data: rdd.RDD[String]): List[String] = {
    val results = data.map(m => {
      val line = SerDe(m)
      val length = line.length
      if (line(length - 1) == "Yes")
        if (length == 8) line(4) else line(3)
    }).collect().distinct.toList
    results.map(x => x.toString)
  }

  
  /** Returns a List[String] of specific placement by index in RDD
    * @param data the rdd you wish to search
    */
  
  def getLine(num: Int, data: rdd.RDD[String]) = {
    val indexedRdd = data.zipWithIndex().map { case (k,v) => (v,k) }
    indexedRdd.lookup(num -1)
  }

  
  /** Returns Int as index of specific column name from a List[String]
    * @param data the rdd you wish to search
    */
  
  def columnIndex(field: String, data: List[String]): Int = {
    data.indexOf(field)
  }

  
  /** Returns a List of unique elements of a specific 'column'. If the 'column' is
    * not found, returns null
    * @param field the 'column' you want to sort
    * @param fieldList the 'fields' available to rdd as a List[String]
    * @param data the rdd you wish to sort
    * ex:
    *    distinctColumn("site_dfa", dfaHeaders, dfaData)
    */
  
  def distinctColumn(field: String, fieldList: List[String], data: rdd.RDD[String]): List[String] = {
    if (fieldList.indexOf(field) > -1)
      data.map(x => SerDe(x)(columnIndex(field,fieldList))).collect().distinct.toList
    else null
  }

  
  /** Returns a RDD String of valid data for a specified RDD
    * @param index string to search
    * @param srcList list of fields you want to check against
    * @param fieldList the 'fields' available to rdd as a List[String]
    * @param data the rdd you wish to sort
    */
  
  def validData(index: String, srcList: List[String], fieldList: List[String], data: rdd.RDD[String]): rdd.RDD[String] =
    data.filter(line => srcList contains SerDe(line)(columnIndex(index, fieldList)))

  
  /** Returns a RDD String of invalid data for a specified RDD
    * @param index string to search
    * @param srcList list of fields you want to check against
    * @param fieldList the 'fields' available to rdd as a List[String]
    * @param data the rdd you wish to sort
    */
  
  def invalidData(index: String, srcList: List[String], fieldList: List[String], data: rdd.RDD[String]): rdd.RDD[String] =
    data.filter(line => !(srcList contains SerDe(line)(columnIndex(index, fieldList))))

  
  /** map/reduces total number for a specific 'column'
    * @param field the rdd you wish to add
    * @param fieldList the 'fields' available to rdd as a List[String]
    * @param data the rdd you wish to sort
    */

  def aggregateColumn(field: String, fieldList: List[String], data: rdd.RDD[String]) =
    data.map(x =>SerDe(x)(columnIndex(field, fieldList)).toInt).
      collect().reduce(_+_)

  
  /** Returns a List of String RDD's grouped by selected rdd 'column', e.g. "campaign"
    * or "site_dfa". If 'column' does not exist, returns null.
    * @param field the 'column' you want to sort by
    * @param data the rdd you wish to sort. dataDfa is the default
    */
  
  def dataBy(field: String, fieldList: List[String], data: rdd.RDD[String]): List[rdd.RDD[String]] = {
    val qualifier = distinctColumn(field, fieldList, data)
    if (qualifier != null) {
      val result = {
        for (q <- qualifier) yield {
          val filtered =
            data.filter(x => SerDe(x)(columnIndex(field, fieldList)) == q)
          for (line <- filtered) yield line
        }
      }
      result
    }
    else null
  }

  
  /** Returns a RDD String of out of desired publisher for a specified RDD
    * @param pub the publisher you want to search for
    * @param fieldList the 'fields' available to rdd as a List[String]
    * @param data the rdd you wish to search. dataDfa is the default
    */
  
  def getPublisher(pub: String, fieldList: List[String], data: rdd.RDD[String]): rdd.RDD[String] =
    data.filter(x => SerDe(x)(columnIndex("site_dfa", fieldList)) == pub)

  
  /** Returns a RDD String filtered by a field in a specific RDD
    * @param field the field you want to search for
    * @param fieldList the 'fields' available to rdd as a List[String]
    * @param criteria the field match you want to search for
    * @param data the rdd you wish to search. dataDfa is the default
    */
  
  def filterPlacements(field: String, fieldList: List[String], criteria: String, data: rdd.RDD[String]): rdd.RDD[String] = {
    if (fieldList.indexOf(field) > -1)
      data.filter(line => {
        SerDe(line)(columnIndex(field, fieldList)) == criteria
      })
    else
      null
  }

  
  /** Returns a RDD String of out of desired buying model for a specified RDD
    * @param model the buy model you want to search for
    * @param data the rdd you wish to search. dataDfa is the default
    */
  
  def getBuyModel(model: String, fieldList: List[String], data: rdd.RDD[String]): rdd.RDD[String] =
    data.filter(x => SerDe(x)(columnIndex("placement_cost_structure", fieldList)) == model)

  
  /** generates a snapshot report of specified data set
    * @param data the data set you wish to report on. dataDFA is the default
    */

  def marketReport(marketList: List[String], fieldList: List[String], data: rdd.RDD[String]): Any = {
    val dataIn: rdd.RDD[String]   = validData("designated_market_area_dma", marketList, fieldList, data)
    val dataOut: rdd.RDD[String]= invalidData("designated_market_area_dma", marketList, fieldList, data)
    val inImps: Int             = aggregateColumn("impressions", fieldList, dataIn)
    val outImps: Int            = aggregateColumn("impressions", fieldList, dataOut)
    val ttlImps: Int            = inImps + outImps
    val pctOutImps: Float       = outImps/ttlImps.toFloat

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