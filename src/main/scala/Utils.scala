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
   * @param params is the Array of args you wish to build from
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
    println(f"\nExecuted in ${((end-start)/1000)} seconds")
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
      if(line(line.length - 1) == "Yes")
        if(line.length == 8) line(4) else line(3)
    }).collect().filterNot(_.toString == "()").distinct.toList

    results.map(x => x.toString)
  }

  
  /** Returns a List[String] of specific placement by index in RDD
    * @param data the rdd you wish to search
    */
  
  def getLine(num: Int, data: rdd.RDD[String]) = {
    val indexedRdd = data.zipWithIndex().map { case (k,v) => (v,k) }
    indexedRdd.lookup(num -1)
  }


  /** returns Int as index of specific column name from a List[String]
    * @param field the 'column' you want to get an index for
    * @param fieldList the 'fields' wish to search
    * ex: columnIndex("date", dfaHeaders)
    */
  def columnIndex(field: String, fieldList: List[String]): Int = {
    fieldList.indexOf(field)
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
      collect().reduce[Int](_+_)

  
  /** Returns a List of String RDD's grouped by selected rdd 'column', e.g. "campaign"
    * or "site_dfa". If 'column' does not exist, returns null.
    * @param field the 'column' you want to sort by
    * @param data the rdd you wish to sort
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
    * @param data the rdd you wish to search
    */
  
  def getPublisher(pub: String, fieldList: List[String], data: rdd.RDD[String]): rdd.RDD[String] =
    data.filter(x => SerDe(x)(columnIndex("site_dfa", fieldList)) == pub)

  
  /** Returns a RDD String filtered by a field in a specific RDD
    * @param field the field you want to search for
    * @param fieldList the 'fields' available to rdd as a List[String]
    * @param criteria the field match you want to search for
    * @param data the rdd you wish to search
    */
  
  def filterPlacements(field: String, fieldList: List[String], criteria: String, data: rdd.RDD[String]): rdd.RDD[String] = {
    if (fieldList.indexOf(field) > -1)
      data.filter(line => {
        SerDe(line)(columnIndex(field, fieldList)) == criteria
      })
    else
      null
  }

  /** Returns a RDD String filtered by specific month in "YYYY-MM" format
    * @param fieldList the 'fields' available to rdd as a List[String]
    * @param data the rdd you wish to search
    */
  
  def filterMonthly(month:String, fieldList: List[String], data: rdd.RDD[String]): rdd.RDD[String] = {
    data.filter(SerDe(_)(columnIndex("date",fieldList)).substring(0,7) == month)
  }
  
  
  /** Returns a RDD String of out of desired buying model for a specified RDD
    * @param model the buy model you want to search for
    * @param data the rdd you wish to search
    */
  
  def getBuyModel(model: String, fieldList: List[String], data: rdd.RDD[String]): rdd.RDD[String] =
    data.filter(x => SerDe(x)(columnIndex("placement_cost_structure", fieldList)) == model)

  
  def placementEssentials(marketList: List[String], fieldList: List[String], data: rdd.RDD[String]): mutable.Map[String, String] = {
    val detailsMap = mutable.Map[String,String]()
    
    val dataIn: rdd.RDD[String] = validData("designated_market_area_dma", marketList, fieldList, data)
    val dataOut: rdd.RDD[String]= invalidData("designated_market_area_dma", marketList, fieldList, data)

    var inImps: Int         = 0
    var inClks: Int         = 0
    var inCTTVLQS: Int      = 0
    var inVTTVLQS: Int      = 0
    var inTVAMCTCONV: Int   = 0
    var inTVAMVTCONV: Int   = 0
    var inTVNCCTCONV: Int   = 0
    var inTVNCVTCONV: Int   = 0
    
    var outImps: Int        = 0
    var outClks: Int        = 0
    var outCTTVLQS: Int     = 0
    var outVTTVLQS: Int     = 0
    var outTVAMCTCONV: Int  = 0
    var outTVAMVTCONV: Int  = 0
    var outTVNCCTCONV: Int  = 0
    var outTVNCVTCONV: Int  = 0

    if(dataIn.count() != 0) {
      inImps        = aggregateColumn("impressions", fieldList, dataIn)
      inClks        = aggregateColumn("clicks", fieldList, dataIn)
      inCTTVLQS     = aggregateColumn("consumer_remarketing_verizon_fios_tv_loopq_success_click_through_conversions", fieldList, dataIn)
      inVTTVLQS     = aggregateColumn("consumer_remarketing_verizon_fios_tv_loopq_success_view_through_conversions", fieldList, dataIn)
      inTVAMCTCONV  = aggregateColumn("consumer_order_confirmation_go_fios_tv_order_am_click_through_conversions", fieldList, dataIn)
      inTVAMVTCONV  = aggregateColumn("consumer_order_confirmation_go_fios_tv_order_am_view_through_conversions", fieldList, dataIn)
      inTVNCCTCONV  = aggregateColumn("consumer_order_confirmation_go_fios_tv_order_nc_click_through_conversions", fieldList, dataIn)
      inTVNCVTCONV  = aggregateColumn("consumer_order_confirmation_go_fios_tv_order_nc_view_through_conversions", fieldList, dataIn)
    }

    if(dataOut.count() != 0) {
      outImps       = aggregateColumn("impressions", fieldList, dataOut)
      outClks       = aggregateColumn("clicks", fieldList, dataOut)
      outCTTVLQS    = aggregateColumn("consumer_remarketing_verizon_fios_tv_loopq_success_click_through_conversions", fieldList, dataOut)
      outVTTVLQS    = aggregateColumn("consumer_remarketing_verizon_fios_tv_loopq_success_view_through_conversions", fieldList, dataOut)
      outTVAMCTCONV = aggregateColumn("consumer_order_confirmation_go_fios_tv_order_am_click_through_conversions", fieldList, dataOut)
      outTVAMVTCONV = aggregateColumn("consumer_order_confirmation_go_fios_tv_order_am_view_through_conversions", fieldList, dataOut)
      outTVNCCTCONV = aggregateColumn("consumer_order_confirmation_go_fios_tv_order_nc_click_through_conversions", fieldList, dataOut)
      outTVNCVTCONV = aggregateColumn("consumer_order_confirmation_go_fios_tv_order_nc_view_through_conversions", fieldList, dataOut)
    }

    val ttlImps: Int          = inImps + outImps
    val ttlClks: Int          = inClks + outClks
    val ttlCTTVLQS: Int       = inCTTVLQS + outCTTVLQS
    val ttlVTTVLQS: Int       = inVTTVLQS + outVTTVLQS
    val ttlTVAMCTCONV: Int    = inTVAMCTCONV + outTVAMCTCONV
    val pctTVAMCTCONV: Float  = inTVAMCTCONV/outTVAMCTCONV.toFloat

    val ttlTVAMVTCONV: Int    = inTVAMVTCONV + outTVAMVTCONV
    val pctTVAMVTCONV: Float  = inTVAMVTCONV/outTVAMVTCONV.toFloat

    val ttlTVNCCTCONV: Int    = inTVNCCTCONV + outTVNCCTCONV
    val pctTVNCCTCONV: Float  = inTVNCCTCONV/outTVNCCTCONV.toFloat

    val ttlTVNCVTCONV: Int    = inTVNCVTCONV + outTVNCVTCONV
    val pctTVNCVTCONV: Float  = inTVNCVTCONV/outTVNCVTCONV.toFloat

    detailsMap.put("ttlImps", ttlImps.toString)
    detailsMap.put("outImps", outImps.toString)
    detailsMap.put("ttlClks", ttlClks.toString)
    detailsMap.put("outClks", outClks.toString)
    detailsMap.put("ttlCTTVLQS", ttlCTTVLQS.toString)
    detailsMap.put("outCTTVLQS", outCTTVLQS.toString)
    detailsMap.put("ttlVTTVLQS", ttlVTTVLQS.toString)
    detailsMap.put("outVTTVLQS", outVTTVLQS.toString)
    detailsMap.put("ttlTVCTCONV", (ttlTVAMCTCONV + ttlTVNCCTCONV).toString)
    detailsMap.put("outTVCTCONV", (outTVAMCTCONV + outTVNCCTCONV).toString)
    detailsMap.put("ttlTVVTCONV", (ttlTVAMVTCONV + ttlTVNCVTCONV).toString)
    detailsMap.put("outTVVTCONV", (outTVAMVTCONV + outTVNCVTCONV).toString)

    detailsMap
  }


  /** Generates a snapshot report of specified data set
    * @param data the data set you wish to report on. dataDFA is the default
    */
  def snapshotReport(marketList: List[String], fieldList: List[String], data: rdd.RDD[String]): List[String] = {
    val results = new ArrayBuffer[String]()
    val months: List[String] = {
      data.map(x =>SerDe(x)(columnIndex("date",fieldList)).substring(0,7)).collect().distinct.toList
    }

    months.foreach(month => {
      val campaigns = distinctColumn("campaign", fieldList, filterMonthly(month, fieldList, data)).sorted
      campaigns.foreach(campaign => {
        val campaignRDD = filterPlacements("campaign", fieldList, campaign, data)
        val publishers = distinctColumn("site_dcm", fieldList, campaignRDD)
        publishers.foreach(publisher => {
          val publisherRDD = filterPlacements("site_dcm", fieldList, publisher, campaignRDD)
          val buyModels = distinctColumn("placement_cost_structure", fieldList, publisherRDD)
          buyModels.foreach(buyModel => {
            val buyModelRDD = filterPlacements("placement_cost_structure", fieldList, buyModel, publisherRDD)
            val report = placementEssentials(marketList, fieldList, buyModelRDD)
            print(".")
            results += (s"$month\t$campaign\t$publisher\t$buyModel\t${report("ttlImps")}\t\t${report("outImps")}" +
              s"\t${report("ttlClks")}\t${report("outClks")}\t${report("ttlCTTVLQS")}\t${report("outCTTVLQS")}" +
              s"\t${report("ttlVTTVLQS")}\t${report("outVTTVLQS")}\t${report("ttlTVCTCONV")}\t${report("outTVCTCONV")}" +
              s"\t${report("ttlTVVTCONV")}\t${report("outTVVTCONV")}")
          })
        })
      })
    })
    println()
    println(s"month\tcampaign\tpublisher\tbuy_model\ttotal_imps\tout_imps\ttotal_clks\tout_clks\ttotal_cttvlqs" +
            s"\tout_cttvlqs\ttotal_vttvlqs\tout_vttvlqs\ttotal_cttvord\tout_cttvord\ttotal_vttvord\tout_vttvord")
    results.toList
  }
}