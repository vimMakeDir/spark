package com.atguigu.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lxk
 * @create 2020-10-31 14:00
 */
object SparkStreaming03_CustomerReceiver {
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")

    //2.初始化sparkstreamingcontext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //创建自定义的streaming
    val lineDstream = ssc.receiverStream(new CustomerReceiver("hadoop102",8888))

    lineDstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()

  }
}


class CustomerReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
 //启动时，读取数据
  override def onStart(): Unit = {
    new Thread("Socket receiver"){
      override def run(){
        receive()
      }
    }.start()
  }

  //读取数据并将数据发送给spark
  def receive():Unit = {
    //创建Socket
    var socket = new Socket(host,port)

    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))

    var input: String = reader.readLine()


    while (!isStopped() && input != null){
      store(input)
      input = reader.readLine()
    }

    reader.close()
    socket.close()

    restart("restart")

  }

  override def onStop(): Unit = {

  }

}