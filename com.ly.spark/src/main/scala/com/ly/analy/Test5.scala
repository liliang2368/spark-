package com.ly.analy

import com.ly.teacherAnalysis.ProduceInfo
import org.apache.spark.{SparkConf, SparkContext}

object Test5 {
  implicit object OrderingProductInfo3 extends Ordering[ProcuctInfo3]{
    override def compare(x: ProcuctInfo3, y: ProcuctInfo3): Int = {
      if(x.amount==y.amount){
        (x.price -y.price).toInt
      }else{
        -(x.amount-y.amount)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("sencond sort").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val productInfo=Array("apple 5 500","pear 2 500","grape 10 500")
    val productRDD=sc.parallelize(productInfo)
    val maped=productRDD.map( item=>{
      val fields=item.split(" ")
      val name=fields(0)
      val price=fields(1).toDouble
      val amount=fields(2).toInt
      new ProduceInfo(name,price,amount)
    })
    val sorted=maped.sortBy(p=>p)
    val result=sorted.collect()
    result.foreach(println)
  }
}
case class ProcuctInfo3(name:String,price:Double,amount:Int)
