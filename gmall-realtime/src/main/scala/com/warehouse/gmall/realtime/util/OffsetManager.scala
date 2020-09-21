package com.warehouse.gmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

object OffsetManager {

  /**
   * 从redis中读取偏移量
   * @param topicName
   * @param groupId
   * @return
   */
  def getOffset( topicName: String, groupId: String ): Map[TopicPartition, Long] ={

    // Redis => type->hash  key->offset:[topic]:[groupid]  field->partition_id value->offset
    // 存入 ->  hmseet offset:GMALL_START:group_dau 0 12 1 15 2 7 3 18
    // 取出 ->  hgetall offset:GMALL_START:group_dau
    val jedis: Jedis = RedisUtil.getJedisClient

    val offsetKey = "offset:" + topicName + ":" + groupId
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)

    jedis.close()

    import scala.collection.JavaConversions._

    val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.map { case (partitionId, offset) =>
      // println( "读取分区：" + partitionId + ":" + offset )
      (new TopicPartition(topicName, partitionId.toInt), offset.toLong)
    }.toMap

    kafkaOffsetMap
  }


  /**
   * 把偏移量写入redis
   * @param topicName
   * @param groupId
   * @param offsetRanges
   */
  def saveOffset( topicName: String, groupId: String, offsetRanges: Array[OffsetRange] ): Unit = {


    val offsetKey = "offset:" + topicName + ":" + groupId

    // 转换结构 -> map
    val offsetMap: util.Map[String, String] = new util.HashMap()
    for (elem <- offsetRanges) {
      val partition: Int = elem.partition
      val untilOffset: Long = elem.untilOffset
      offsetMap.put( partition + "", untilOffset + "" )
      // println( "写入分区：" + partition + ":" + elem.fromOffset + "--->" + elem.untilOffset )
    }

    // 写入redis
    if( offsetMap != null && offsetMap.size() > 0){
      val jedis: Jedis = RedisUtil.getJedisClient
      jedis.hmset( offsetKey,  offsetMap)
      jedis.close()
    }

  }

}
