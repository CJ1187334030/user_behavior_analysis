package com.atguigu.order_timeout_detect

import com.atguigu.order_timeout_detect.bean._
import org.apache.flink.cep.scala._
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object OrderTimeoutDetect {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "pay", 1558430844)
    )).assignAscendingTimestamps(_.eventTime * 1000)

    val orderBykey: KeyedStream[OrderEvent, Long] = orderEventStream.keyBy(_.orderId)

    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .next("next").where(_.eventType == "pay").within(Time.seconds(15))

    //定义侧输出流
    val timeoutTag: OutputTag[OrderResult] = OutputTag[OrderResult]("outputTimeout")

    val patternDS: PatternStream[OrderEvent] = CEP.pattern(orderBykey,pattern)



    import scala.collection.Map
    val dst: DataStream[OrderResult] = patternDS.select(timeoutTag)(
      (patternT: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val id: Long = patternT.getOrElse("begin", null).iterator.next().orderId
        OrderResult(id, "timeout")
      }
    )(
      (patternS: Map[String, Iterable[OrderEvent]]) => {
        val id: Long = patternS.getOrElse("next", null).iterator.next().orderId
        OrderResult(id, "success")
      }
    )


    //正常匹配的
    dst.print("success")

    //timeout的侧输出流
    val sidePut: DataStream[OrderResult] = dst.getSideOutput(timeoutTag)
    sidePut.print("timeout")


    env.execute()

  }

}
