import org.apache.spark.{SparkConf, SparkContext}

/**
 * 版本一 求每一个省的访问次数
 */
object IpTest1 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("ip match").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val path="/Users/mac/data/ip.txt"
    val path_city="/Users/mac/data/access.log"
    val rules=utils.readRules(path)//本地数据  driver 端：内存数受限
    //access.log 日志文件比较大  集群运算
    val lines=sc.textFile(path_city)
    //解析 lines中的每一行 取出 Ip转成十禁止  再去rules匹配到对应的地址
      val proviceAndOne=lines.map(line=>{
        val fields=line.split("\\|")//只需要取出第一个字段
        val ip=fields(1)
        val ipNum=utils.ip2Long(ip)
        //查找
        var index=utils.binarySearch(rules,ipNum)
        //取出省名
        var provice = "unknow"
        if(index != -1){
        val ipRule=rules(index)
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
