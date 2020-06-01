package com.ly.otherRDDAnaltsis

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object jdbcRdd {
  val genConn=()=>{
    DriverManager.getConnection("jdbc:mysql://node1:3306/bigdata","root","a")

  }

  def main(args: Array[String]): Unit = {

    //     local  -->本机测试环境       spark-shell
    val conf=new SparkConf().setAppName("scala word count").setMaster("local[*]")
    val sc=new SparkContext(conf)

    val jdbcRDD=new JdbcRDD(sc,genConn,"select *from ipresult where id>=? and id<=?",6,10,2,row=>{
      (row.getInt(1),row.getString(2),row.getInt(3))
    })
      val result=jdbcRDD.collect()
    result.foreach(println)

  }
}
