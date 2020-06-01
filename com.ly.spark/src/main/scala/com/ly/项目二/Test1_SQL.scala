package com.ly.项目二

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}


/**
 * *  1。读取信息,统计数据条数   职业数 电影数 用户数 评分条数
 * *  2.显示 每个职业下用户详细信息 显示为 :(职业编号,(人的编号,性别,年龄为分析,职业名))
 *  1. "ratings.dat" UserID::MovieID::Rating::Timestamp
 *  2."users.dat" :UserID::Gender::Age::OccupationID::Zip-code
 *  3. "movies.dat":MovieID::Title::Genres
 *  4,"occupations.dat":occupationID::OccupationName
 */
object Test1_SQL {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARNING)//配置文件
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()
    //统计电影数
    val lines= spark.read.textFile("data/movies.dat")
    val lines_occ= spark.read.textFile("data/occupations.dat")
    val lines_user= spark.read.textFile("data/users.dat")

    import spark.implicits._
    val movieDataSet=lines.map(line=>{
      val fields=line.split("::")
      (fields(0),fields(1),fields(2))
    })
    val moviesDataFrame=movieDataSet.toDF("MovieID","Title","Genres")
//    moviesDataFrame.createTempView("movies")
    //将其转换成sql表
    val occupationsDataSet=lines_occ.map(line=>{
      val fields=line.split("::")
      (fields(0),fields(1))
    })
    val occuaptionFrame=occupationsDataSet.toDF("occupationID","OccupationName")
//    occuaptionFrame.createTempView("occupations")
    val userDataSet=lines_user.map(line=>{
      val fields=line.split("::")
      (fields(0),fields(1),fields(2),fields(3))
    })
    val userFrame=userDataSet.toDF("UserID","Gender","Age","OccupationID")
//    userFrame.createTempView("users")

    //使用sql来统计电影的数目
    println("电影的总数为")
//    spark.sql("select count(*) from movies").show()
   println( moviesDataFrame.count())
    println("职业数")
//    spark.sql("select count(*) from occupations").show()
    print("用户数")
//    spark.sql("select count(*) from users").show()
    println(" * *  2.显示 每个职业下用户详细信息 显示为 :(职业编号,(人的编号,性别,年龄为分析,职业名))")
//    occuaptionFrame.select(userFrame,$"OccupationID"===$"occupationID")
//    spark.sql("select a.occupationID ,b.UserID,b.Gender,b.Age,a.OccupationName from occupations a " +
//      "inner join users b on (a.OccupationID==b.occupationID) where a.OccupationName='artist'").show()

//    spark.sql("select b.occupationID, count(*) from occupations a " +
//      "inner join users b on (a.OccupationID==b.occupationID) where a.OccupationName='artist' group by b.occupationID").show()
//






  }

}
