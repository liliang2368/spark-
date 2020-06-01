package com.ly.otherRDDAnaltsis

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    RDD
    //     local  -->本机测试环境       spark-shell
    val conf=new SparkConf().setAppName("scala word count").setMaster("local[*]")
    val sc=new SparkContext(conf)
  }
}
