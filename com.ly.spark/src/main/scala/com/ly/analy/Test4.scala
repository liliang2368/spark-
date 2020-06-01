package com.ly.analy

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 单一科目下的数据也无法在内存中排序
 * 解决方案 :
 *      Excetor来排序 另外要将数据(分组聚合之后的数据放到不同的分区上)
 */

object Test4 {
  def main(args: Array[String]): Unit = {
    var path="/Users/mac/data/visit.log"
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
      val httpHost=line.substring(0,index)
      val subject=new URL(httpHost).getHost().split("\\.")(0)

      ((subject,page),1)
    })
    //这聚合产生了一次shuff的操作      然后下面的按f
  //  val rdd3=rdd2.reduceByKey(_+_)
   // val subjects=Array("bigdata","javaee","front")
    //取出科目名 形成集合 用于构造分区器也是聚合
    //rdd3中取出 subject 驱虫转成数组
   val subjects= rdd2.map(_._1._1).distinct().collect() //动作操作

    //创建分区器
    val subjectPartitioner=new SubjectPartitioner(subjects)
    //在聚合的时候就开始分区   这样的好处是shuff的操作是减少的 shuff的操作是及其消耗内存
     val partitionRDdd= rdd2.reduceByKey(subjectPartitioner,_+_)
    //分好区的数据可以进行排序
    val result=partitionRDdd.mapPartitions(it=>{
      it.toList.sortBy(_._2).reverse.take(4).iterator
    }  )
    result.foreach(println)
//
//
//    val rdd4=rdd3.sortBy(_._2,false)
//    val result=rdd4.collect()
//    result.foreach(println)
//    sc.stop()
  }


}




