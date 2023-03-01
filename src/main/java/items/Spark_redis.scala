package  items
import Util.Jedis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}


object Spark_redis{

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Bzip2Parquet").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val df: DataFrame = spark.read.parquet("E:\\s\\白华强\\互联网项目\\output")
    // 创建临时视图
    df.createTempView("log")
    // 编写sql语句  ,.
    val sql = "select provincename,cityname,count(1) as ct from log group by provincename,cityname";
    val resDF: DataFrame = spark.sql(sql)
    val rdd: RDD[Row] = resDF.rdd
    rdd.foreachPartition(res => {
      // 连接redis
      val jedis = Jedis.getJedis
      res.foreach(row => {
        val pn: String = row.getAs[String]("provincename")
        val cn: String = row.getAs[String]("cityname")
        val ct: Long = row.getAs[Long]("ct")
        var key = pn + "|" + cn
        var value = pn + "," + cn + "," + ct
        jedis.set(key, value)
      })
      jedis.close()
    })
    spark.stop()


  }

}
