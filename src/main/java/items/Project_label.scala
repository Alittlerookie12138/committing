package items

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Project_label {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Parquet").master("local[*]").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val df: DataFrame = spark.read.parquet("E:\\s\\白华强\\互联网项目\\output")
    val value: RDD[((String, String), List[Double])] = df.rdd.map(row => {
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val province: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")
      val appname: String = row.getAs[String]("appname")

      // 调用方法.
      val yyqqs: List[Double] = DimTool.ysqqRpt(requestmode, processnode)
      val cyjjs: List[Double] = DimTool.jiangjiaRpt(iseffective, isbilling, isbid, iswin, adorderid)
      val ggzss: List[Double] = DimTool.ggzjRtp(requestmode, iseffective)
      val mjzss: List[Double] = DimTool.mjjRtp(requestmode, iseffective, isbilling)
      val ggxf: List[Double] = DimTool.ggcbRtp(iseffective, isbilling, iswin, winprice, adpayment)

      ((province, cityname), yyqqs ++ cyjjs ++ ggzss ++ mjzss ++ ggxf)
    })
    value

    value.reduceByKey((list1,list2)=>{
      list1.zip(list2).map(t=>t._1+t._2)
    }).foreach(println(_))

  }

}
