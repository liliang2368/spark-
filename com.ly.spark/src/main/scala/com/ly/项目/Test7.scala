package com.ly.项目

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
/*
数据描述
*  1. "ratings.dat" UserID::MovieID::Rating::Timestamp
*  2."users.dat" :UserID::Gender::Age::OccupationID::Zip-code
*  3. "movies.dat":MovieID::Title::Genres
*  4,"occupations.dat":occupationID::OccupationName
*
*/
object Test7 {
  def main(args: Array[String]): Unit = {


    //先读取这四个数据
    val conf = new SparkConf().setAppName("ip match").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val path_user = "/Users/mac/data/users.dat"
    val path_occupation = "/Users/mac/data/occupations.dat"
    val path_movies = "/Users/mac/data/movies.dat"
    val path_ratings = "/Users/mac/data/ratings.dat"
    sc.textFile(path_ratings).map(line => {
      val filds = line.split("::")
      val fm = new SimpleDateFormat("yyyy")
      val tim = fm.format(new Date(filds(3).toLong))
      (tim, 1)
    }).reduceByKey(_+_).foreach(println)
  }
}