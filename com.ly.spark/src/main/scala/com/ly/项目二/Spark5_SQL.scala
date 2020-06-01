package com.ly.项目二

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 *
 *1. "ratings.dat" UserID::MovieID::Rating::Timestamp
 * *  2."users.dat" :UserID::Gender::Age::OccupationID::Zip-code
 * *  3. "movies.dat":MovieID::Title::Genres
 * *  4,"occupations.dat":occupationID::OccupationName
 *
 *
 */
object Spark5_SQL {
  def main(args: Array[String]): Unit = {
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
    //1.Api方案 分析最受不同年龄段欢迎的前十条
    ratDataFrame.createTempView("v_rat")
    moviesDataFrame.createTempView("v_movies")
    userDataFrame.createTempView("v_user")
    spark.sql("select a.MovieID,Title,Genres,count(a.MovieID) count,avg(Rating) rat from v_rat a " +
      "inner join v_user b on(a.UserID==b.UserID)  " +
      "inner join v_movies c on(a.MovieID==c.MovieID) where Age=1" +
      " group by a.MovieID,Title,Genres " +
      " order by rat desc,count desc limit 5").show()
    println("API")
    import org.apache.spark.sql.functions._ //sql中支持的内置函数   (  max, min,avg, count, to_date,........ substr )
    ratDataFrame.join(userDataFrame,userDataFrame("UserID")===ratDataFrame("UserID"))
      .join(moviesDataFrame,ratDataFrame("MovieID")===moviesDataFrame("MovieID"))
      .where($"Age"===1)
      .groupBy(ratDataFrame("MovieID"),$"Title",$"Genres")
      .agg(avg($"Rating").as("rat"),count($"Title").as("count"))
      .sort($"rat".desc,$"count".desc).show(3)


  }

}
