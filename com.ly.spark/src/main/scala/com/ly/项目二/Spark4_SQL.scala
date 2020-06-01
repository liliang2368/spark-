package com.ly.项目二

/**
 * * *  1。读取信息,统计数据条数   职业数 电影数 用户数 评分条数
 * * *  2.显示 每个职业下用户详细信息 显示为 :(职业编号,(人的编号,性别,年龄为分析,职业名))
 * *  1. "ratings.dat" UserID::MovieID::Rating::Timestamp
 * *  2."users.dat" :UserID::Gender::Age::OccupationID::Zip-code
 * select avg(Rating),countMovieID,Title,Genres  from ratings a
 * inner join users b on(a.UserID==b.UserID)
 * inner join movies c on(c.MovieID==a.MovieID)
 * group by MovieID,Title,Genres, where b.Gender='M'
 *不同职业下面对观看类型的影响
 *
 * *  3. "movies.dat":MovieID::Title::Genres
 * *  4,"occupations.dat":occupationID::OccupationName
 */

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Spark4_SQL {
  def main(args: Array[String]): Unit = {
   // Logger.getLogger("org").setLevel(Level.OFF)//配置文件
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
   val userDataSet=spark.sparkContext.textFile(path_user).map(lines=>{
     val fields=lines.split("::")
     Row(fields(0),fields(1),fields(2),fields(3),fields(4))
   })
    val userSchema=StructType("UserID::Gender::Age::OccupationID::Zip-code".split("::")map(line=>{
      StructField(line,StringType,true)
    }))
    //创建
    val userDataFrame=spark.createDataFrame(userDataSet,userSchema)
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
    userDataFrame.createTempView("v_user")
    println(" 需求4:分析男性用户最喜欢看的前十部电影")
    //SQL方案
    spark.sql("select a.MovieID,Title,Genres,avg(Rating) rat,count(a.MovieID) count from v_rat a " +
      "inner join v_user b on(a.UserID==b.UserID) " +
      "inner join v_movies c on(c.MovieID==a.MovieID) " +
      "where b.Gender='M' " +
      "group by a.MovieID,Title,Genres " +
      "order by rat desc,count desc limit 10").show()
    import org.apache.spark.sql.functions._ //sql中支持的内置函数   (  max, min,avg, count, to_date,........ substr )
    println("API方法调用")
    //API方案
    ratDataFrame
      .join(userDataFrame,ratDataFrame("UserID")===userDataFrame("UserID"))
      .join(moviesDataFrame,ratDataFrame("MovieID")===moviesDataFrame("MovieID"))
      .where($"Gender"==="M")
      .groupBy(ratDataFrame("MovieID"),moviesDataFrame("Title"),moviesDataFrame("Genres"))
      .agg(avg("Rating").as("rat"),count("Genres").as("count"))
      .sort($"rat".desc,$"count".desc)
      .select(ratDataFrame("MovieID"),$"Title",$"Genres",$"rat",$"count")
      .show(10)






  }
}
