package items

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object abc {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Parquet").master("local[*]").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val df: DataFrame = spark.read.parquet("E:\\s\\白华强\\互联网项目\\output")
    df.createTempView("emp")
    val frame: DataFrame = spark.sql("select imei,mac,idfa,openudid,androidid,imeimd5,macmd5,idfamd5,openudidmd5,androididmd5,imeisha1,macsha1,idfasha1,openudidsha1,androididsha1 from emp")
    val rdd: RDD[Row] = frame.rdd
    rdd.saveAsTextFile("E:\\s\\白华强\\互联网项目\\abc")

    spark.stop()
    sc.stop()

  }

}
