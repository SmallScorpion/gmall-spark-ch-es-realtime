package com.warehouse.gmall.realtime.dwd

import com.alibaba.fastjson
import com.alibaba.fastjson.{JSON, JSONObject}
import com.warehouse.gmall.realtime.bean.OrderInfo
import com.warehouse.gmall.realtime.util.{MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object OrderInfoApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("order_indo_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext( sparkConf, Seconds(5) )

    val topic: String = "SPARK_CH_ES_REALTIME_GMALL_ODS_ORDER_INFO"
    val groupId = "SPARK_CH_ES_REALTIME_GMALL_ORDER_INFO_GROUP"

    // TODO 读取Redis偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset( topic, groupId )

    // TODO 消费kafka数据
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null

    if( kafkaOffsetMap != null && kafkaOffsetMap.nonEmpty ) {
      recordInputStream = MyKafkaUtil.getKafkaStream( topic, ssc, kafkaOffsetMap, groupId )
    } else {
      recordInputStream = MyKafkaUtil.getKafkaStream( topic, ssc, groupId )
    }

    // recordInputStream.map( _.value() ).print()

    // TODO 得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var OffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] =
      recordInputStream.transform { rdd =>
        // driver 执行
        OffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }

    // TODO 基本结构转换，补时间字段
    val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      val timeArr: Array[String] = createTimeArr(1).split(":")
      orderInfo.create_hour = timeArr(0)
      orderInfo
    }


    // TODO 查询hbase用户状态
    val orderInfoWithFirstFlagDStream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>

      // 获取每个分区所有的userId
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if(orderInfoList.nonEmpty) {


        val userIdList: List[Long] = orderInfoList.map( _.user_id )

        // 查询一批user_id , 将其转换为Map

        val sql: String = "select user_id,if_consumed from spark_ch_es_realtime_user_state where user_id in (" + userIdList.mkString("','") + ")"
        val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)

        val userStateMap: Map[String, String] = userStateList.map(jsonObj => (jsonObj.getString("USER_ID"), jsonObj.getString("IF_CONSUMED"))).toMap

        // 将每个User_id 获取对应HBASE中订单状态作出判断是否为首单
        for (orderInfo <- orderInfoList) {

          // 是否消费
          val if_consumed: String = userStateMap.getOrElse(orderInfo.user_id.toString, null)
          // 如果是消费用户， 首单标志为0
          if (if_consumed != null || if_consumed == "1") {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }

        }
      }
      orderInfoList.toIterator

    }


    orderInfoWithFirstFlagDStream.print(1000)

/*

       // TODO 查询hbase用户状态
    orderInfoDstream.map{ orderInfo =>

      val sql: String = "select user_id,if_consumed from spark_ch_es_realtime_user_state where user_id='" + orderInfo.user_id + "'"
      val userStateList: List[JSONObject] = PhoenixUtil.queryList( sql )

      // 处理数据，判断是否为首单
      if( userStateList != null && userStateList.nonEmpty ) {

        val userStateJsonObj: JSONObject = userStateList.head
        if( userStateJsonObj.getJSONObject( "IF_CONSUMED" ).equals("1") ) {
          orderInfo.if_first_order = "0"
        } else {
          orderInfo.if_first_order = "1"
        }

      } else {
        orderInfo.if_first_order= "1"
      }
    }


    orderInfoWithFirstFlagDStream.print(1000)

*/




    // 通过用户状态为订单增加首单标志

    // 维度数据的合并

    // 保存用户状态
    // 保存订单明细

    ssc.start()
    ssc.awaitTermination()


  }

}
