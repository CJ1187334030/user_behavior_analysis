package com.atguigu.login_fail_detect

import com.atguigu.login_fail_detect.bean.LoginEvent
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailDetectWithCep {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    val loginEventStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000L)

    val loginKeyBy: KeyedStream[LoginEvent, Long] = loginEventStream.keyBy(_.userId)

    //定义匹配模式
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail").within(Time.seconds(2))

    //再数据流中匹配出定义好模式
    val patternDS: PatternStream[LoginEvent] = CEP.pattern(loginKeyBy,pattern)


    //.select 出入pattern select function ， 检测到定义好的模式序列就会匹配
    import scala.collection.Map
    val lDS: DataStream[LoginEvent] = patternDS.select(
      (patternse: Map[String, Iterable[LoginEvent]]) =>
        patternse.getOrElse("next", null).iterator.next()
    )

    lDS.print()

    env.execute()


  }



}
