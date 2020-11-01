package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lxk
 * @create 2020-11-01 10:21
 */
object sparkStreaming06_updateStateByKey {

  //定义updateFunc
  def updateFunc = (seq:Seq[Int],state:Option[Int]) =>{
    val currentCount = seq.sum
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }

  //不能新new,不然都是新对象闯将
  def createSSC():StreamingContext = {
    //1.创建sparkconf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    //2.创建streamingcontext
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    ssc.checkpoint("./ck")

    val line = ssc.socketTextStream("hadoop102",9999)
    val wordToOne = line.flatMap(_.split(" ")).map((_,1))

    //6.使用updatestatebykey来更新状态，统计从运行开始以来的单词的总次数
    wordToOne.updateStateByKey[Int](updateFunc).print()
    ssc
  }

  def main(args: Array[String]): Unit = {

   val ssc = StreamingContext.getActiveOrCreate("./ck",() =>createSSC())


    //7 开启任务
    ssc.start()
    ssc.awaitTermination()

  }
}
