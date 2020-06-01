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
object Test9 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ip match").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val path_user = "/Users/mac/data/users.dat"
    val path_occupation = "/Users/mac/data/occupations.dat"
    val path_movies = "/Users/mac/data/movies.dat"
    val path_ratings = "/Users/mac/data/ratings.dat"
    //统计不同职业观看的习惯 (Rating,(职业,1))
    val occupationRDD=sc.textFile(path_occupation)
    val userRDD=sc.textFile(path_user)
    val RatRDD=sc.textFile(path_ratings)
    //先求出所有的职业
    val zhiye=occupationRDD.map(line=>{
      val fileds=line.split("::")
      (fileds(0),fileds(1))
    }).collect()
    //不同职业
    for(i<-zhiye){
        sc.textFile(path_ratings).map(line=>{
          val fileds=line.split("::")
          (fileds(0).toInt,fileds(2))
        }).join(
          sc.textFile(path_user).filter(line=>{
            val fileds=line.split("::")
            if(fileds(3)==i._1){
              true
            }else{
              false
            }
          }).map(line=>{
            val fileds=line.split("::")
            (fileds(0).toInt,i._2)
            //类型:(职业:1)
          })).map(x=>(x._2._1,(x._2._2,1))).reduceByKey((p1,p2)=>{
          (i._2,p1._2+p2._2)
        }).foreach(println)
    }






  }
}
