package com.ly.项目

import org.apache.spark.{SparkConf, SparkContext}
/*
数据描述
*  1. "ratings.dat" UserID::MovieID::Rating::Timestamp
*  2."users.dat" :UserID::Gender::Age::OccupationID::Zip-code
*  3. "movies.dat":MovieID::Title::Genres
*  4,"occupations.dat":occupationID::OccupationName
*
*/
object Test2 {
  def main(args: Array[String]): Unit = {
    //先读取这四个数据
    val conf = new SparkConf().setAppName("ip match").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val path_user = "/Users/mac/data/users.dat"
    val path_occupation = "/Users/mac/data/occupations.dat"
    val path_movies = "/Users/mac/data/movies.dat"
    val path_ratings = "/Users/mac/data/ratings.dat"
    val ratingRDD=sc.textFile(path_ratings)
    val moviesRDD=sc.textFile(path_movies)
    val ratMap=ratingRDD.map(line=>{
      val fields=line.split("::")
      (fields(1),fields(0))
    })
    val moviesMap=moviesRDD.map(line=>{
      val fields=line.split("::")
      (fields(0),(fields(1),fields(2)))
    })
    //(2834,(5649,(Very Thought of You, The (1998),Comedy|Romance)))
    val mapswap=ratMap.join(moviesMap)
    mapswap.map(line=>(line._2._1,(line._1,line._2._2._1,line._2._2._2))).foreach(println)
  }
}
