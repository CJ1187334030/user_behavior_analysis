package com.atguigu.login_fail_detect


import com.atguigu.login_fail_detect.bean.LoginEvent
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


object LoginFailDetect {

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
      .assignAscendingTimestamps(_.eventTime * 1000)
      .filter(_.eventType == "fail")
      .keyBy(_.userId)
      .process(new MyKeyedProcessFunction)
        .print()

    env.execute()


  }

  class MyKeyedProcessFunction extends KeyedProcessFunction[Long,LoginEvent,LoginEvent] {


    lazy val failState:ListState[LoginEvent] =
      getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("failState",classOf[LoginEvent]))

    override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, collector: Collector[LoginEvent]): Unit = {

      failState.add(i)
      //设定定时器，2s后触发
      context.timerService().registerEventTimeTimer(i.eventTime + 2*1000)

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {


      val allogin = new ListBuffer[LoginEvent]

      import scala.collection.JavaConversions._
      for(i <- failState.get()){
        allogin += i
      }

      failState.clear()

      if(allogin.length >1)

        out.collect(allogin.head)


    }

  }

}
