package com.atguigu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lxk
 * @create 2020-11-01 10:05
 */
object SparkStreaming05_Transform {
  def main(args: Array[String]): Unit = {
    //1.创建sparkconf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    //2.创建sparkstreamingcontext
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //3.具体逻辑
    var dStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    println("111111111:" + Thread.currentThread().getName)

    val wordToSumDstream = dStream.transform(
      rdd => {
        println("222222:" + Thread.currentThread().getName)

        var words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordToOne = words.map(
          x => {
            println("333333:" + Thread.currentThread().getName)
            (x, 1)
          })
        val value = wordToOne.reduceByKey(_ + _)
        value
      }
    )
    wordToSumDstream.print()


    //4.启动
    ssc.start()
    ssc.awaitTermination()
  }
}
