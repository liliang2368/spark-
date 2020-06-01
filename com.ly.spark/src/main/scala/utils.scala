import java.util

import scala.io.{BufferedSource, Source}

object utils {

  /**
   * 单机查找
   */
  def main(args: Array[String]): Unit = {
  //   List list=new util.ArrayList[String]();
      val ipNum=ip2Long("1.0.1.0")
   //   println(ipNum)
      val ipRules=readRules("/Users/mac/data/ip.txt")
     // ipRules.foreach(println)
      val index =binarySearch(ipRules,ipNum)
      println(index)
    }


  /**
   * 将ip地址转换成长整型
   * @param ip
   */
  def ip2Long(ip:String):Long={
    //测试
    val fragments=ip.split("\\.")
   // fragments.foreach(println)
    var ipNum=0l
    for (i<-0 until fragments.length){
      ipNum=fragments(i).toLong | ipNum<<8L
    }
    ipNum
  }
  //读取规则
  def readRules(path:String):Array[IpRule]={
    val bf:BufferedSource=Source.fromFile(path)
     val lines:Iterator[String]=bf.getLines()
    //scala :map单机
    //spark 集群
    val rules:Array[IpRule]=lines.map( line=>{
      val fields=line.split("\\|")
      //TODO:目前只解析了 startIP,endIP province
      val startIpLong=fields(2).toLong
      val endIpLong=fields(3).toLong
      val province=fields(6)
      new IpRule(startIpLong,endIpLong,province)
    } ).toArray
    rules
    }

  /**
   * 到给定的ip规则 (ipRules列表 中查找 ip 返回下标)
   */
  def binarySearch(ipRules:Array[IpRule],ip:Long):Int={
    var low = 0
    var high=ipRules.length-1
    while(low<=high){
      var middle=(low+high)/2
      if( (ip>=ipRules(middle).getStartIP) && (ip<=ipRules(middle).getEndIP)  ){
       //   已经找着了
        return middle
      }else if(ip<ipRules(middle).getStartIP){
        high =middle-1
      }else{
        low=middle+1
      }
    }
    -1
  }

}
