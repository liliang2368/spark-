package com.ly.项目二

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * 分析每年度生产的电影数目
 */
object Spark_7SQL {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARNING)//配置文件
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()
    val path_user = "data/users.dat"
    val path_occupation = "data/occupations.dat"
    val path_movies = "data/movies.dat"
    val path_ratings = "data/ratings.dat"
    import spark.implicits._
    //先解析所有的数据
    val ratDataSet=spark.sparkContext.textFile(path_ratings).map(lines=>{
      val files=lines.split("::")
      Row(files(0),files(1),files(2).toInt,files(3))
    })
    val ratSchema=StructType("UserID::MovieID".split("::").map(column => StructField(column,StringType,true)))
      .add("Rating",IntegerType,true)
      .add("Timestamp",StringType,true)
    //组装成dataFrame
    val ratDataFrame=spark.createDataFrame(ratDataSet,ratSchema)
    val moviesDataSet=spark.sparkContext.textFile(path_movies).map(line=>{
      val fileds=line.split("::")
      Row(fileds(0),fileds(1),fileds(2))
    })
    val moviesScame=StructType("MovieID::Title::Genres".split("::").map(column=>{
      StructField(column,StringType,true)
    }))
    val moviesDataFrame=spark.createDataFrame(moviesDataSet,moviesScame)
    //1.Api方案
    ratDataFrame.createTempView("v_rat")
    moviesDataFrame.createTempView("v_movies")
    spark.udf.register("parse",(Title:String)=>{
      Title.substring(0,Title.lastIndexOf(")")+1).substring(Title.lastIndexOf("("))
    })
    //需要解析电影总数
    spark.sql("select parse(Title) as year ,count(parse(Title)) from v_movies group by parse(Title) order by year asc").show()





  }
}
