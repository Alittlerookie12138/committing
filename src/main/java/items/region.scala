package items

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

object region {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("Parquet").master("local[*]").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val df: DataFrame = spark.read.parquet("E:\\s\\白华强\\互联网项目\\output")
    // 创建临时视图
    df.createTempView("log")
    // 编写sql语句  ,.
    val sql = "select provincename,cityname,count(1) as ct from log group by provincename,cityname";

    val resDF: DataFrame = spark.sql(sql)

    // 如何将结果写出.
    // 判断输出路径是否存在。如果存在，就删除
    val config: Configuration = sc.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(config)
    val path = new Path("E:\\s\\白华强\\互联网项目\\outBY")
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    resDF.write.partitionBy("provincename","cityname").json("E:\\s\\白华强\\互联网项目\\outBY")

    spark.stop()
    sc.stop()


  }

}
