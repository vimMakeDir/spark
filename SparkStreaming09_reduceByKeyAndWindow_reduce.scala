package com.atguigu.sparkstreaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lxk
 * @create 2020-11-01 11:16
 */
object SparkStreaming09_reduceByKeyAndWindow_reduce {
  def main(args: Array[String]): Unit = {
    //1 创建SparkConf
        val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    //2 创建StreamingContext
        val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint("./ck1")

    val linesDstream = ssc.socketTextStream("hadoop102",9999)
    val wordToOne = linesDstream.flatMap(_.split(" ")).map((_,1))
    val value = wordToOne.reduceByKeyAndWindow(
      (x: Int, y: Int) => (x + y),
      (a: Int, b: Int) => (a - b),
      Seconds(12),
      Seconds(6),
      new HashPartitioner(2),
      (x:(String,Int)) => x._2 > 0
    )
    value.print()



    //4 开启任务
        ssc.start()
        ssc.awaitTermination()



  }
}
