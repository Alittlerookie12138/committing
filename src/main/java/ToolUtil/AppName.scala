package ToolUtil

import org.apache.spark.sql.{DataFrame, SparkSession}

object AppName {
  def appname(id:String,app:Int): Unit ={
    val spark: SparkSession = SparkSession.builder().appName("Parquet").master("local[*]").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val df: DataFrame = spark.read.parquet("E:\\s\\白华强\\互联网项目\\output")
    df.createTempView("AppName")
    spark.sql("select * from AppName where ")
  }

}
