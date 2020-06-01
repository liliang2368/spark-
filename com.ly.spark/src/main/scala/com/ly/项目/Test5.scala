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
object Test5 {
  def main(args: Array[String]): Unit = {
    //先读取这四个数据
    val conf = new SparkConf().setAppName("ip match").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val path_user = "/Users/mac/data/users.dat"
    val path_occupation = "/Users/mac/data/occupations.dat"
    val path_movies = "/Users/mac/data/movies.dat"
    val path_ratings = "/Users/mac/data/ratings.dat"
    //将年龄分成三种 年轻 中年 老年
    val userRDD=sc.textFile(path_user)
    val user=userRDD.filter(line=>{
    val fields=line.split("::")
      if(fields(2).toInt>1  && fields(2).toInt<=20){
        true
      }else {
        false
      }
    }).map(line=>{
      val fields=line.split("::")
      (fields(0).toInt,"广大青年")
    })
  //  user.foreach(println)
    //（userID,电影id）
    val ratRDD=sc.textFile(path_ratings)
    ratRDD.map(line=>{
      val fields=line.split("::")
      (fields(0).toInt,1)
    }).reduceByKey(_+_).sortBy(x=>x._2).join(user).take(10).foreach(println)






  }
}
