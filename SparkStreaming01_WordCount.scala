package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lxk
 * @create 2020-10-31 11:33
 */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //1.初始化spark配置信息
    var sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")

    //2.初始化sparkstreamingcontext
    var ssc: StreamingContext = new StreamingContext(sparkconf, Seconds(3))

    //2.1 通过监控端口创建Dstream,
    var lineDstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    //对数据进行wordcount
    lineDstream.flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_ + _)
        .print()


    //3.开启
    ssc.start()

    //4.将线程阻塞
    ssc.awaitTermination()


  }
}
