package items

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object Project_label {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Parquet").master("local[*]").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val df: DataFrame = spark.read.parquet("E:\\s\\白华强\\互联网项目\\output")
    val value: RDD[(String, Int, String, String, Int, String, String,String)] = df.rdd.map(row => {
      val imei: String = row.getAs[String]("imei")
      val mac: String = row.getAs[String]("mac")
      val idfa: String = row.getAs[String]("idfa")
      val openudid: String = row.getAs[String]("openudid")
      val androidid: String = row.getAs[String]("androidid")
      val imeimd5: String = row.getAs[String]("imeimd5")
      val macmd5: String = row.getAs[String]("macmd5")
      val idfamd5: String = row.getAs[String]("idfamd5")
      val openudidmd5: String = row.getAs[String]("openudidmd5")
      val androididmd5: String = row.getAs[String]("androididmd5")
      val imeisha1: String = row.getAs[String]("imeisha1")
      val macsha1: String = row.getAs[String]("macsha1")
      val idfasha1: String = row.getAs[String]("idfasha1")
      val openudidsha1: String = row.getAs[String]("openudidsha1")
      val androididsha1: String = row.getAs[String]("androididsha1")
      val adspacetype: Int = row.getAs[Int]("adspacetype")
      val adspacetypename: String = row.getAs[String]("adspacetypename")
      val appname: String = row.getAs[String]("appname")
      val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
      val device: String = row.getAs[String]("device")
      val adplatformkey: String = row.getAs[String]("adplatformkey")
      val keywords: String = row.getAs[String]("keywords")
      val ID: String = ToolUtil.LabelTool.itm(imei, mac, idfa, openudid, androidid, imeimd5, macmd5, idfamd5, openudidmd5, androididmd5, imeisha1, macsha1, idfasha1, openudidsha1, androididsha1)
      (ID, adspacetype, adspacetypename, appname, adplatformproviderid, device, adplatformkey,keywords)
    })
//    value.filter(x=>x._1!="").foreach(println(_))
    value.foreach(println(_))
    spark.stop()
    sc.stop()

  }

}
