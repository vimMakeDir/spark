package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lxk
 * @create 2020-11-01 11:06
 */
object SparkStreaming08_reduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    //1 创建SparkConf
        val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    //2 创建StreamingContext
        val ssc = new StreamingContext(conf, Seconds(3))


    //监控端口数据
    val Dstream = ssc.socketTextStream("hadoop102",9999)
    val wordToOne = Dstream.flatMap(_.split(" "))
      .map((_, 1))

    //算法逻辑，窗口12秒，滑步6秒
    val wordCounts = wordToOne.reduceByKeyAndWindow((a:Int,b:Int) => (a + b),Seconds(12),Seconds(6))

    wordCounts.print()


    //4 开启任务
        ssc.start()
        ssc.awaitTermination()



  }
}
