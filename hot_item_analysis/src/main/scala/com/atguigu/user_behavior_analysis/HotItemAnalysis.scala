package com.atguigu.user_behavior_analysis


import java.sql.Timestamp
import com.atguigu.user_behavior_analysis.bean.{ItemViewCount, UserBehavior}
import com.atguigu.user_behavior_analysis.utils.MykafkaCon
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItemAnalysis {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //替换kafka数据源
    val value: FlinkKafkaConsumer[String] = MykafkaCon.getKafkaCon("test")

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

    val dsProcess: DataStream[String] = dsItemViewCountKeyBy.process(new MyKeyedProcessFunction(3))

    dsProcess.print()

    env.execute()


  }

  class  MyKeyedProcessFunction(topSize:Int) extends  KeyedProcessFunction[Tuple,ItemViewCount,String] {

    //定义状态
    private var itemState:ListState[ItemViewCount] = _


    override def open(parameters: Configuration): Unit = {

      super.open(parameters)

      //命名状态变量的名字和类型
      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState",classOf[ItemViewCount])
      itemState = getRuntimeContext.getListState(itemStateDesc)

    }


    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {

      itemState.add(i)

      //注册定时器，触发时间为 windowend + 1
      context.timerService().registerEventTimeTimer(i.windowEnd + 1)

    }

    //定时器触发操作，从state取出数据，排序取出topN ，输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

      //获取所有商品点击信息
      val allItems = new ListBuffer[ItemViewCount]

      import scala.collection.JavaConversions._
      for( item <- itemState.get){
        allItems += item
      }

      //清空
      itemState.clear()

      //按照点击量从大到小排序
      val sortedItem: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

      val result = new StringBuilder
      result.append("============================\n")
      result.append("时间：").append(new Timestamp(timestamp -1 )).append("\n")

      for (i <- sortedItem.indices) {

        val currentItem = sortedItem(i)

        // e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i+1).append(":")
          .append("  商品ID=").append(currentItem.itemId)
          .append("  浏览量=").append(currentItem.count).append("\n")

      }

      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)

    }

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
