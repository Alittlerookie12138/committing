import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Parquet_sql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("sql").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    val frame: DataFrame = spark.read.parquet("E:\\s\\白华强\\项目\\output")
    frame.createTempView("emp")
    val df: DataFrame = spark.sql("select * from emp limit 3")
    df.show()

    spark.stop()
    sc.stop()
  }
}
