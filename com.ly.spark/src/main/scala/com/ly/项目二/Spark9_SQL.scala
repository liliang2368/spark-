package com.ly.项目二

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 *
 * * *  1。读取信息,统计数据条数   职业数 电影数 用户数 评分条数
 * * *  2.显示 每个职业下用户详细信息 显示为 :(职业编号,(人的编号,性别,年龄为分析,职业名))
 * *  1. "ratings.dat" UserID::MovieID::Rating::Timestamp
 * *  2."users.dat" :UserID::Gender::Age::OccupationID::Zip-code
 * select avg(Rating),countMovieID,Title,Genres  from ratings a
 * inner join users b on(a.UserID==b.UserID)
 * inner join movies c on(c.MovieID==a.MovieID)
 * group by MovieID,Title,Genres, where b.Gender='M'
 *
 * *  3. "movies.dat":MovieID::Title::Genres
 * *  4,"occupations.dat":occupationID::OccupationName
 */
object Spark9_SQL {
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
    val userDataSet=spark.sparkContext.textFile(path_user).map(line=>{
      val fileds=line.split("::")
      Row(fileds(0),fileds(1),fileds(2),fileds(3),fileds(4))
    })
    val userScame=StructType("UserID::Gender::Age::OccupationID::Zip-code".split("::").map(column=>{
      StructField(column,StringType,true)
    }))
    val userDataFrame=spark.createDataFrame(userDataSet,userScame)
    val occDataSet=spark.sparkContext.textFile(path_occupation).map(line=>{
      val fileds=line.split("::")
      Row(fileds(0),fileds(1))
    })
    val occScame=StructType("occupationID::occupationName".split("::").map(column=>{
      StructField(column,StringType,true)
    }))
    val occDataFrame=spark.createDataFrame(occDataSet,occScame)
    val moviesWithGenres = moviesDataFrame.flatMap(row => {
      val movieid=row.getString(0)
      val title=row.getString(1)
      val genres = row.getString(2)
      val types=genres.split("\\|")
      for(i<-0 until types.length) yield (movieid, title, types(i))
    }).toDF("MovieID","Title","Genres")
    //1.Api方案
    ratDataFrame.createTempView("v_rat")
    moviesWithGenres.createTempView("v_movies")
    occDataFrame.createTempView("v_occ")
    userDataFrame.createTempView("v_user")
    println("不同职业下面对观看类型的影响")
    spark.sql("select Genres,count(Genres) from v_rat a " +
      "inner join v_user b on(a.UserID=b.UserID) " +
      "inner join v_occ c on (b.occupationID=c.occupationID) " +
      "inner join v_movies d on (a.MovieID=d.MovieID) " +
      "where c.occupationName='artist' " +
      "group by d.Genres" +
      "").show()

  }
}
