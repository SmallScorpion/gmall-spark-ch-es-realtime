package com.warehouse.gmall.realtime.app

import com.warehouse.gmall.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext}

object DauApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext( sparkConf, Seconds(5) )

    val topic: String = "GMALL_SPARK_CK_ES_START"
    val recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream( topic, ssc )


    recordInputStream.map( _.value() ).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
