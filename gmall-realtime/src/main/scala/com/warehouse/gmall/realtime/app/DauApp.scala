package com.warehouse.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.warehouse.gmall.realtime.bean.DauInfo
import com.warehouse.gmall.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * 日活
 */
object DauApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext( sparkConf, Seconds(5) )


    val topic: String = "GMALL_SPARK_CK_ES_START"
    val groupId = "DAU_GROUP"

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

    // TODO redis去重
    val jsonObjDstream: DStream[JSONObject] = InputGetOffsetDstream.map { record =>
      // 提取日志
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      // 提取事件戳
      val ts: lang.Long = jsonObj.getLong("ts")
      val datehourString: String = new SimpleDateFormat("yyyy-MM-dd HH")
        .format(new Date(ts))
      val dateHour: Array[String] = datehourString.split(" ")
      // 提取时间戳
      jsonObj.put("dt", dateHour(0))
      jsonObj.put("hr", dateHour(1))

      jsonObj
    }
/*

    // 利用redis保存今天访问过系统的用户清单
    // 清单在redis中保存
    // string(k-v) hash( k-(k,v) ) list set zset(k-v-s(排序评分))
    // redis ： type->Set  key->dau-2020-06-17 value->mid
    val filterDstream: DStream[JSONObject] = jsonObjDstream.filter { jsonObj =>
      // 获取字段
      val dt: String = jsonObj.getString("dt")
      val mid: String = jsonObj.getJSONObject("common").getString("mid")
      // 获取连接
      val jedis: Jedis = RedisUtil.getJedisClient
      val dauKey: String = "dau:" + dt
      // 如果为存在则保存，返回1，如果已存在则不保存 返回0
      val isNew = jedis.sadd(dauKey, mid)
      jedis.close()
      if (isNew == 1L) {
        true
      } else {
        false
      }
    }

*/
    val filterDstream: DStream[JSONObject] = jsonObjDstream.mapPartitions { jsonObjItr =>

      // 获取连接:一个分区只申请一个连接
      val jedis: Jedis = RedisUtil.getJedisClient
      // 结果集
      val filterList = new ListBuffer[JSONObject]
      val jsonList: List[JSONObject] = jsonObjItr.toList
      //println("过滤前" + jsonList.size)
      for (jsonObj <- jsonList) {
        // 获取字段
        val dt: String = jsonObj.getString("dt")
        val mid: String = jsonObj.getJSONObject("common").getString("mid")

        val dauKey: String = "dau:" + dt
        // 如果为存在则保存，返回1，如果已存在则不保存 返回0
        val isNew = jedis.sadd(dauKey, mid)
        // 过期日期
        jedis.expire( dauKey, 3600 * 24 )
        if (isNew == 1L) {
          filterList += jsonObj
        }
      }

      jedis.close()
      //println("过滤后" + jsonList.size)
      filterList.toIterator

    }


    // TODO ES的批量保存
    filterDstream.foreachRDD { rdd =>

      rdd.foreachPartition { jsonItr =>

        val jsonList: List[JSONObject] = jsonItr.toList

        // 转换结构
        val dauList: List[ (String, DauInfo) ] = jsonList.map { jsonObj =>
          val commonJSONObj: JSONObject = jsonObj.getJSONObject("common")
          val dauInfo = DauInfo(
            commonJSONObj.getString("mid"),
            commonJSONObj.getString("uid"),
            commonJSONObj.getString("ar"),
            commonJSONObj.getString("ch"),
            commonJSONObj.getString("vc"),
            jsonObj.getString("dt"),
            jsonObj.getString("hr"),
            jsonObj.getLong("ts")
          )
          ( dauInfo.mid, dauInfo )
        }

        // 获取当前日期
        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format( new Date() )
        // 批量保存
        MyEsUtil.bulkDoc(dauList, "gmall_ch_dau_info" + dt)

      }

      // TODO 提交偏移量
      OffsetManager.saveOffset( topic, groupId, OffsetRanges )

    }

    ssc.start()
    ssc.awaitTermination()

  }

}
