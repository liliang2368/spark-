import org.apache.spark.{SparkConf, SparkContext}

object test1 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象  设置appName和master地址
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    //创建 SparkContext对象 这个对象很重要 它会创建 DAGScheduler
    val sc = new SparkContext(sparkConf)
    //输出日志级别
    sc.setLogLevel("WARN")
    //    //读取数据文件
    //    val data:RDD[String]=sc.textFile("/Users/mac/spark/spark-2.4.5-bin-hadoop2.6/README.md")
    //    println(data)
    val raw = List("a", "b", "d", "f", "g", "h", "o", "1", "x", "y")
    val rdd=sc.parallelize(raw)
    val result=rdd.aggregate((0,0))(
      (acc,str)=>{
        var big=acc._1
        var small=acc._2
        if(str.compareTo("f")>0) {
          big=acc._1+1
        }
        if (str.compareTo("f")<0) {
          small=acc._2+1
        }
        (big,small)
      },
      (x,y)=>(x._1+y._1,x._2+y._2)
    )
    println(result)
  }
}
