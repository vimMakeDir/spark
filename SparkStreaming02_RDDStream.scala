package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author lxk
 * @create 2020-10-31 11:51
 */
object SparkStreaming02_RDDStream {
  def main(args: Array[String]): Unit = {
    //1.初始化spark配置信息
    var sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    //2.初始化sparkstreamingcontext
    var ssc: StreamingContext = new StreamingContext(sparkconf, Seconds(4))

    //3.创建rdd队列
    val queue = new mutable.Queue[RDD[Int]]

    //4.创建QueueInputDstream
    var inputDstream: InputDStream[Int] = ssc.queueStream(queue,false)

    //将队列中的元素进行聚合,并打印
    inputDstream.reduce(_ + _ ).print()

    //启动任务
    ssc.start()

    //循环向队列中放入数据
    for(i <- 1 to 5){
      queue += ssc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(2000)
    }


    //阻塞等待
    ssc.awaitTermination()

  }

}
