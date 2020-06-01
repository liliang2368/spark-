package com.ly.otherRDDAnaltsis

import java.io.{DataOutputStream, FileOutputStream}

class TaskDescription (
  val taskId:Long,
  val attempNumber:Int,
  val executorId:String,
  val name:String,
  val index:Int,
  val partitionId:Int
                      ){
  override def toString: String = "TaskDesciption(TTid=%d,index=%d)".format(taskId,index)

}
object Test{
  def main(args: Array[String]): Unit = {
    val taskDescription=new TaskDescription(0l,1,"1","任务一",0,2)
    println(taskDescription)
    val bytesOut=new FileOutputStream("")
    val dataOut=new DataOutputStream(bytesOut)
    dataOut.writeLong(taskDescription.taskId)
  //  dataOut.writeInt(taskDescription.taskId)

  }
}
