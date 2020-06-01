package com.ly.项目

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object Test8 {
  def main(args: Array[String]): Unit = {
   // (1970,(type,count))
    //先读取这四个数据
    val conf = new SparkConf().setAppName("ip match").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val path_user = "/Users/mac/data/users.dat"
    val path_occupation = "/Users/mac/data/occupations.dat"
    val path_movies = "/Users/mac/data/movies.dat"
    val path_ratings = "/Users/mac/data/ratings.dat"
    //先把年龄进行分组
    sc.textFile(path_ratings).map(line => {
      val filds = line.split("::")
      val fm = new SimpleDateFormat("yyyy")
      val tim = fm.format(new Date(filds(3).toLong))
      (filds(2),(tim , 1))
    }).reduceByKey ( (p1, p2) =>{
      (p1._1,p1._2+p2._2)
    }
    ).foreach(println)
  }
}
