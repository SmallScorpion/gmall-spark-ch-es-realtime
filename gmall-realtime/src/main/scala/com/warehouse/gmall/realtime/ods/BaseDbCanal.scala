package com.warehouse.gmall.realtime.ods

import com.alibaba.fastjson
import com.alibaba.fastjson.JSON
import com.warehouse.gmall.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import scala.util.parsing.json.JSONObject

object BaseDbCanal {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("base_db_canal_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext( sparkConf, Seconds(5) )


    val topic: String = "SPARK_CH_ES_REALTIME_GMALL_DB_C"
    val groupId = "BASE_DB_CANAL_GROUP"

    // TODO 读取Redis偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset( topic, groupId )

    // TODO 消费kafka数据
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null

    if( kafkaOffsetMap != null && kafkaOffsetMap.nonEmpty ) {
      recordInputStream = MyKafkaUtil.getKafkaStream( topic, ssc, kafkaOffsetMap, groupId )
    } else {
      recordInputStream = MyKafkaUtil.getKafkaStream( topic, ssc )
    }

    // recordInputStream.map( _.value() ).print()

    // 得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var OffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val InputGetOffsetDstream: DStream[ConsumerRecord[String, String]] =
      recordInputStream.transform { rdd =>
        // driver 执行
        OffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    // 提取数据
    val jsonObjDstream: DStream[fastjson.JSONObject] = InputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: fastjson.JSONObject = JSON.parseObject(jsonString)
      jsonObj
    }

    jsonObjDstream.foreachRDD { rdd=>
      // 推回Kafka
      rdd.foreach { jsonObj =>
        // 提取被操作的数据
        //{"data":[{"id":"1307855165842644994","user_id":"143","sku_id":"14","spu_id":"11","order_id":"3464","appraise":"1202","comment_txt":"评论内容：15383558841713133168573997679436695873694336977279","create_time":"2020-09-21 09:32:24","operate_time":null},{"id":"1307855165842644995","user_id":"183","sku_id":"16","spu_id":"12","order_id":"3465","appraise":"1204","comment_txt":"评论内容：84912178565213194279657317935824289377938624727318","create_time":"2020-09-21 09:32:24","operate_time":null},{"id":"1307855165842644996","user_id":"123","sku_id":"14","spu_id":"11","order_id":"3466","appraise":"1204","comment_txt":"评论内容：17393478361626554984571742467591265887775958195457","create_time":"2020-09-21 09:32:24","operate_time":null},{"id":"1307855165842644997","user_id":"
        val jsonArr = jsonObj.getJSONArray("data")

        // 发送kafka的topic(根据表名)
        val tableName: String = jsonObj.getString("table")
        val topic = "ODS_" + tableName.toUpperCase

        // 遍历输送到k afka
        import scala.collection.JavaConversions._
        for( json <- jsonArr) {
          // 提取数据
          val msg: String = json.toString
          // 发送 -> 非幂等，可能导致数据重复
          MyKafkaUtil.send( topic, msg )
        }

      }
      // 提交偏移量
      OffsetManager.saveOffset( topic, groupId, OffsetRanges )
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
