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
object Test4 {
  def main(args: Array[String]): Unit = {
    //先读取这四个数据
    val conf = new SparkConf().setAppName("ip match").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val path_user = "/Users/mac/data/users.dat"
    val path_occupation = "/Users/mac/data/occupations.dat"
    val path_movies = "/Users/mac/data/movies.dat"
    val path_ratings = "/Users/mac/data/ratings.dat"
    //先分析男性用户
    val userRDD=sc.textFile(path_user)
    val user=userRDD.map(line=>{
      val fields=line.split("::")
      (fields(0),fields(1))
    }).filter{case (k,v) => v=="F"}
    //（电影id,用户）
    val ratRDD=sc.textFile(path_ratings)
    val rat=ratRDD.map(line=>{
      val fields=line.split("::")
      (fields(0),1)
    }).reduceByKey(_+_).sortBy(x=>x._2).join(user).map(x=>(x._2._1,x._1))
    //找出所有的电影名
    //(电影id,电影名)
    val moviesRDD=sc.textFile(path_movies)
    val movice=moviesRDD.map(line=>{
      val fileds=line.split("::")
      (fileds(0).toInt,fileds(1))
    })
    rat.join(movice).take(10).foreach(println)



  }
}
