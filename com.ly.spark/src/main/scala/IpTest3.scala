import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 版本三 通过spark去存数据
 * 假如ip.txt文件feichangsa
 * 解决方案 将 ip.txt存之 hdfs  2。利用spark来读取
 *
 *
 */
object IpTest3 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("ip match").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val path="/Users/mac/data/ip.txt"
    val path_city="/Users/mac/data/access.log"
    //TODO使用spark集群来读取
    val rulesLines=sc.textFile(path)//这是数据是一个RDD 而不是一个Array
   // rulesLines.filter()
    val ipRulesRdd:RDD[IpRule]=rulesLines.map(line=>{
      //line就是指一行 ip规则 按 \切分 取第三个列和第四个列的值
      val fields=line.split("\\|")
      val startIpLong=fields(2).toLong
      val endIpLong=fields(3).toLong
      val provice=fields(6)
      new IpRule(startIpLong,endIpLong,provice)
    })

    //广播变量的使用规则 rdd是不能广播
    val ruleInDriver=ipRulesRdd.collect()//collect将 excutor结构汇总到driver端
    //val rules=utils.readRules(path)//本地数据  driver 端：内存数受限
    val broadCastRef=sc.broadcast(ruleInDriver)
    //access.log 日志文件比较大  集群运算
    val lines=sc.textFile(path_city)
    //解析 lines中的每一行 取出 Ip转成十禁止  再去rules匹配到对应的地址
    val proviceAndOne=lines.map(line=>{
      val fields=line.split("\\|")//只需要取出第一个字段
      val ip=fields(1)
      val ipNum=utils.ip2Long(ip)
      //TODO 从广播变量取值
      val value=broadCastRef.value
      //查找
      var index=utils.binarySearch(value,ipNum)
      //取出省名
      var provice = "unknow"
      if(index != -1){
        val ipRule=value(index)
        provice=ipRule.getProvince()
      }
      //省对一
      (provice,1)
    })
    //将 它编程器 (省,1)
    //再规约
    val reduced=proviceAndOne.reduceByKey((v1,v2)=>v1+v2)
    reduced.foreach(println)
    sc.stop()
  }

}
