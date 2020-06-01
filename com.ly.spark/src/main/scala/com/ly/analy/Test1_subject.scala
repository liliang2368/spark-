package com.ly.analy

import org.apache.spark.{SparkConf, SparkContext}

object Test1_subject {
  def main(args: Array[String]): Unit = {
    var path="db/visit.log"
    if(args.length>=1){
      path=args(0)
    }
    val conf=new SparkConf().setAppName("tearch").setMaster("local[2]")
    val sc=new SparkContext(conf)

    //读文件
    val lines=sc.textFile(path)
    val rdd2=lines.map( line=>{
      val index=line.lastIndexOf("/")
      val page=line.substring(index+1)
      (page,1)
    })
    //聚合
    val rdd3=rdd2.reduceByKey(_+_)
    val rdd4=rdd3.sortBy(_._2,false)
    val result=rdd4.collect()
    result.foreach(println)
    sc.stop()
  }
}
