package com.ly.teacherAnalysis

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 数据: 产品名 单价   销量
 * 需求 :按销量来进行排序 相同   则按单价进行升序
 * // sql :多列进行排序
 */
object Test5 {
  def main(args: Array[String]): Unit = {
     val conf=new SparkConf().setAppName("sencond sort").setMaster("local[*]")
     val sc=new SparkContext(conf)
     val productInfo=Array("apple 5 500","pear 2 500","grape 10 500")
      val productRDD=sc.parallelize(productInfo)
      val maped=productRDD.map(item =>{
      val fields=item.split(" ")
      val name=fields(0)
      val price=fields(1).toDouble
      val amount=fields(2).toInt
      new ProduceInfo(name,price,amount)
    })
    val sorted=maped.sortBy(p =>p )
    val result=sorted.collect()
    result.foreach(println)
  }
}
case class  ProduceInfo(name:String,price:Double,amount:Int) extends Ordered[ProduceInfo]{
  override def compare(that: ProduceInfo): Int = {
    if (this.amount== that.amount){
      (this.price-that.price).toInt
    }else{
      -(this.amount-that.amount)
    }
  }
}
