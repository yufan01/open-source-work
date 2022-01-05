package com.BigData.spark

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}

object spark_code {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MyApp")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/BigData.Data")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/BigData.Result")
      .getOrCreate()
    // 设置log级别

    spark.sparkContext.setLogLevel("WARN")
    val df: DataFrame = MongoSpark.load(spark)
    df.show()
    df.createOrReplaceTempView("user")
    val resRdd1 = spark.sql("select location,price from user").rdd
    resRdd1.foreach(println)
    val resRdd2=resRdd1.map(row=>row.mkString).map(
      _.split(']')
    ).map(
      x=>{
        val line=x(0).split('\'')(1)
        (line,x(1).toInt)
      }
    )
    resRdd2.foreach(println)

    val finalResult=resRdd2.groupByKey()
      .map(
        info=>{
          var sum:Double=0
          var count = 0
          val it = info._2.iterator
          while (it.hasNext){
            sum=sum+it.next()
            count=count+1
          }
          val avg=sum/count
          (info._1,avg)
        }
      )
    finalResult.foreach(println)
    import spark.implicits._
    val resultDf=finalResult.toDF("area","avgPrice")
    MongoSpark.save(resultDf)
    spark.stop()
  }

}
