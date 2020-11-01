package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lxk
 * @create 2020-11-01 10:44
 */
object SparkStreaming07_window {
  def main(args: Array[String]): Unit = {
    //1 创建SparkConf
        val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    //2 创建StreamingContext
        val ssc = new StreamingContext(conf, Seconds(3))

    //3.监控端口9999
    var lineDstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    val lines = lineDstream.flatMap(_.split(" ")).map((_,1))
    //第一个参数  窗口时间   第二个参数   滑步时间
    var wordToOneByWindow: DStream[(String, Int)] = lines.window(Seconds(12), Seconds(6))
    //对数据进行聚合并答应
    wordToOneByWindow.reduceByKey(_ + _ ).print()




    //4 开启任务
        ssc.start()
        ssc.awaitTermination()

  }

}
