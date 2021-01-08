package com.atguigu.user_behavior_analysis


import java.lang

import com.atguigu.user_behavior_analysis.bean.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HotItemAnalysis {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dsUB: DataStream[UserBehavior] = env.readTextFile("D:\\study\\IDEA\\user_behavior_analysis\\hot_item_analysis\\src\\main\\resources\\UserBehavior.csv")
      .map {
        log =>
          val strings: Array[String] = log.split(",")
          UserBehavior(strings(0).toLong, strings(1).toLong, strings(2).toInt, strings(3), strings(4).toLong)
      }
      .assignAscendingTimestamps(_.timestamp*1000)
        .filter(_.behavior == "pv")

    //或 dsUB.keyBy(1)  Tuple为聚合字段
    val dskeyBy: KeyedStream[UserBehavior, Tuple] = dsUB.keyBy("itemId")

    val dsWin: WindowedStream[UserBehavior, Tuple, TimeWindow] = dskeyBy.timeWindow(Time.hours(1),Time.minutes(5))

    //每个商品再每个窗口里面的数据流  (1,[11:00 11:00),3)
    val dsItemViewCount: DataStream[ItemViewCount] = dsWin.aggregate(new MyAggregateFunction,new MyWindowFunction)

    val dsItemViewCountKeyBy: KeyedStream[ItemViewCount, Tuple] = dsItemViewCount.keyBy("windowEnd")





    env.execute()


  }




  //包别引错
  class MyWindowFunction extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow] {

    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0

      val count: Long = input.iterator.next()

      out.collect(ItemViewCount(itemId,window.getEnd,count))

    }
  }



  //预处理，累加器
  class MyAggregateFunction extends AggregateFunction[UserBehavior,Long,Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

}
