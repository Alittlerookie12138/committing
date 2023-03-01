
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import com.redislabs.provider.redis._
import org.apache.spark.rdd.RDD

import org.apache.spark.{SparkConf, SparkContext}

object Spark_redis {

  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf()
      .setAppName("datasource_redis")
      .set("spark.redis.host", "127.0.0.1")
      .set("spark.redis.port", "6379")
//    .set("spark.redis.auth", "")  没有密码可不要这句
      .set("spark.driver.allowMultipleContexts","true").setMaster("local[*]"))


    val stringRedisData:RDD[(String,String)] = sparkContext.parallelize(Seq[(String,String)](("tom","189")))
    sparkContext.toRedisKV(stringRedisData)
  }
}
