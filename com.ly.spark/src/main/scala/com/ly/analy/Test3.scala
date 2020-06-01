package com.ly.analy

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 单一科目下的数据也无法在内存中排序
 * 解决方案 :
 *      Excetor来排序 另外要将数据(分组聚合之后的数据放到不同的分区上)
 */

object Test3 {
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
    //聚合
    val rdd3=rdd2.reduceByKey(_+_)
   // val subjects=Array("bigdata","javaee","front")
    //取出科目名 形成集合 用于构造分区器
    //rdd3中取出 subject 驱虫转成数组
   val subjects= rdd3.map(_._1._1).distinct().collect() //动作操作

    //创建分区器
    val subjectPartitioner=new SubjectPartitioner(subjects)
    //将 rdd3中的数据要分区
     val partitionRDdd= rdd3.partitionBy(subjectPartitioner)
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
//RDD有五个属性 其中分区是 K V
class SubjectPartitioner( subjects:Array[String]  ) extends Partitioner {
 //相当于主构造方法的逻辑代码
  val map=new mutable.HashMap[String,Int]() //科目 分区
  var i=0
  for (s<-subjects){
      map.put(s,i)
  }
  //分区表中有多少数量 由科目数决定
  override def numPartitions: Int = {
    subjects.length
  }
  // (subject,page)
  override def getPartition(key: Any): Int = {
      val subjectname= key.asInstanceOf[(String,String)]._1
      map(subjectname)
  }
}




