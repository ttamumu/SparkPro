import java.sql.DriverManager
import java.sql.DriverManager._

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tt on 2018/10/25.
  */
object JdbcRDDDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //定义mysql信息
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://192.168.31.88:3306/alarm_database",
        "dbtable" -> "(select * from person) as some_alias",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> "root",
        //"partitionColumn"->"day_id",
        "lowerBound" -> "0",
        "upperBound" -> "1000",
        //"numPartitions"->"2",
        "fetchSize" -> "100",
        "password" -> "tddx123456")).load()
        jdbcDF.collect()

  }
}
