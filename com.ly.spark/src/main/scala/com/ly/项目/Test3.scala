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
object Test3 {
  def main(args: Array[String]): Unit = {
    //先读取这四个数据
    val conf = new SparkConf().setAppName("ip match").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val path_user = "/Users/mac/data/users.dat"
    val path_occupation = "/Users/mac/data/occupations.dat"
    val path_movies = "/Users/mac/data/movies.dat"
    val path_ratings = "/Users/mac/data/ratings.dat"
    println("==============统计电影中平均得分最高的十部电影=============")
    val moviesRDD=sc.textFile(path_movies)
    val movice= moviesRDD.map(line=>{
      val fileds=line.split("::")
      (fileds(0),fileds(1))
    })
   // movice.take(10).foreach(println)
    val RatRDD=sc.textFile(path_ratings)
   val moviceRat= RatRDD.map(line=>{
      val fileds=line.split("::")
      (fileds(1),(fileds(2).toInt,1))
    }).reduceByKey((p1,p2)=>{
     ((p1._1+p2._1),p1._2+p2._2)
   }).map(item=>(item._1,(item._2._1)/(item._2._2).toDouble)).join(movice).map(x=>(x._2._1,x._2._2)).sortByKey(false)
    moviceRat.take(10).foreach(println)
    println("============统计观看人数最多的10部电影============")
    //（人数,电影id）
    val ratRDD=sc.textFile(path_ratings)
    ratRDD.map(line=>{
      val fields=line.split("::")
      (fields(1),1)
    }).reduceByKey(_+_).sortBy(x=>x._2).join(movice).sortBy(p=>p._2._1,false).map(x=>(x._2._1,x._2._2)).take(10).foreach(println)
  }
}
