package com.ly.项目二

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/*
数据描述
*  1. "ratings.dat" UserID::MovieID::Rating::Timestamp
*  2."users.dat" :UserID::Gender::Age::OccupationID::Zip-code
*  3. "movies.dat":MovieID::Title::Genres
*  4,"occupations.dat":occupationID::OccupationName
*
*/
object Test3_SQL {
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
    println("==============统计电影中平均得分最高的十部电影=============")
    //写sql
    spark.sql("select Title,a.MovieID,avg(Rating) as Rat ,count(a.MovieID) as mm from v_movies a " +
      "inner join v_rat b on(a.MovieID==b.MovieID) group by TiTle,a.MovieID order by Rat desc, mm desc limit 3" ).show()
    println("============统计观看人数最多的10部电影 输出详细电影信息============")
    spark.sql("select Title,a.MovieID,avg(Rating) as Rat ,count(a.MovieID) as mm from v_movies a " +
      "inner join v_rat b on(a.MovieID==b.MovieID) group by TiTle,a.MovieID order by mm desc limit 3" ).show()
    //2.DCL方案
    println("DSl方案")
    import org.apache.spark.sql.functions._ //sql中支持的内置函数   (  max, min,avg, count, to_date,........ substr )
    ratDataFrame.select("MovieID","Rating")
     // .withColumnRenamed("MovieID","mid")
      .join(moviesDataFrame,ratDataFrame("MovieID")===moviesDataFrame("MovieID"))//两表连接
      .groupBy($"TiTle",ratDataFrame("MovieID"))
      .agg(avg(ratDataFrame("Rating")).as("avgrating"), count(ratDataFrame("MovieID")).as("cns"))//统计次数
      .sort($"avgrating".desc,$"cns".desc)
      .select("Title","MovieID","avgrating","cns")
      .show(3)

  }
}
