package com.ly.项目二

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * 分析某年度下不同类型的电影总数
 */
object  Spark8_SQL {
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
    val moviesWithGenres = moviesDataFrame.flatMap(row => {
      val movieid=row.getString(0)
      val title=row.getString(1)
      val genres = row.getString(2)
      val types=genres.split("\\|")
      for(i<-0 until types.length) yield (movieid, title, types(i))
    }).toDF("MovieID","Title","Genres")
    moviesWithGenres.show()
  //  val moviesDataFrame=spark.createDataFrame(moviesDataSet,moviesScame)

    //1.Api方案
    ratDataFrame.createTempView("v_rat")
    moviesWithGenres.createTempView("v_movies")
    spark.udf.register("parse",(Title:String)=>{
      Title.substring(0,Title.lastIndexOf(")")).substring(Title.lastIndexOf("(")+1)
    })
    spark.sql("select Genres,count(Genres) from v_movies where parse(Title)=1976 group by Genres").show()
  }
}
