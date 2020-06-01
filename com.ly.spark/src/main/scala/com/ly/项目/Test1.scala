package com.ly.项目

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求一：
 *  1。读取信息,统计数据条数   职业数 电影数 用户数 评分条数
 *  2.显示 每个职业下用户详细信息 显示为 :(职业编号,(人的编号,性别,年龄为分析,职业名))
 *
 *  电影点评系统用户行为分析   用户观看电影和点评电影所有的行为数据采集 过滤 处理和展示
 *
 *  数据描述
 *  1. "ratings.dat" UserID::MovieID::Rating::Timestamp
 *  2."users.dat" :UserID::Gender::Age::OccupationID::Zip-code
 *  3. "movies.dat":MovieID::Title::Genres
 *  4,"occupations.dat":occupationID::OccupationName
 *
 */
object Test1 {
  def main(args: Array[String]): Unit = {


    //先读取这四个数据
    val conf = new SparkConf().setAppName("ip match").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val path_user = "/Users/mac/data/users.dat"
    val path_occupation = "/Users/mac/data/occupations.dat"
    //第一步 以职业编号做键
    val userRDD = sc.textFile(path_user)
    val occupationRDD=sc.textFile(path_occupation)
    val userMap = userRDD.map(line => {
      val fileds = line.split("::")
      (fileds(3), (fileds(0), fileds(1), fileds(2), fileds(4)))
    })
    val occMap=occupationRDD.map(line=>{
      val fileds=line.split("::")
      (fileds(0),fileds(1))
    })
    userMap.join(occMap).foreach(println)
   // userMap.collect().foreach(println)
  }







}
