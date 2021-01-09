package com.atguigu.network_traffic_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.network_traffic_analysis.bean.{ApacheLogEvent, UrlViewCount}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object NetworkTrafficAnalysis {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //83.149.9.216 - - 17/05/2015:10:05:33 +0000 GET /presentations/logstash-monitorama-2013/images/nagios-sms5.png
    val ds: DataStream[ApacheLogEvent] = env.readTextFile("D:\\study\\IDEA\\user_behavior_analysis\\network_traffic_analysis\\src\\main\\resources\\apachetest.log")
      .map {
        log =>
          val strings: Array[String] = log.split(" ")
          //获取时间
          val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
          val time: Long = format.parse(strings(3)).getTime

          ApacheLogEvent(strings(0), strings(2), time, strings(5), strings(6))
      }
      //乱序时间处理  设置时间戳和水位
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) {
      override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
    })

    val dsKeyBy: KeyedStream[ApacheLogEvent, Tuple] = ds.keyBy("url")

    val dsWin: WindowedStream[ApacheLogEvent, Tuple, TimeWindow] = dsKeyBy.timeWindow(Time.minutes(1),Time.seconds(5))

    val dsAggre: DataStream[UrlViewCount] = dsWin.aggregate(new MyAggregateFunction,new MyWindowFunction)

    val dsKeyByWin: KeyedStream[UrlViewCount, Tuple] = dsAggre.keyBy("windowEnd")

    val dsKeyPro: DataStream[String] = dsKeyByWin.process(new MyKeyedProcessFunction(5))

    dsKeyPro.print()

    env.execute()


  }


  class MyKeyedProcessFunction(topSize:Int) extends KeyedProcessFunction[Tuple,UrlViewCount,String] {

    //定义状态变量
    private var urlState:ListState[UrlViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)

      //命名状态变量名字和类型
      val urlStateDesc = new ListStateDescriptor[UrlViewCount]("urlState",classOf[UrlViewCount])
      urlState = getRuntimeContext.getListState(urlStateDesc)

    }

    //注册定时器，时间为windowEnd + 1
    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Tuple, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {

      urlState.add(i)
      context.timerService().registerEventTimeTimer(i.windowEnd + 1)

    }

    //定时器触发操作，从state取出数据 排序取前tipN
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

      val allUrl = new ListBuffer[UrlViewCount]

      import scala.collection.JavaConversions._

      for (state <- urlState.get){
        allUrl += state
      }

      urlState.clear()

      val urlSort: ListBuffer[UrlViewCount] = allUrl.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

      val result = new StringBuilder
      result.append("============================\n")
      result.append("时间：").append(new Timestamp(timestamp -1 )).append("\n")

      for (i <- urlSort.indices) {

        val currentItem = urlSort(i)

        // e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i+1).append(":")
          .append("  URL=").append(currentItem.url)
          .append("  浏览量=").append(currentItem.count).append("\n")

      }

      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)

    }

  }

  class MyWindowFunction extends WindowFunction[Long,UrlViewCount,Tuple,TimeWindow] {

    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {

      val url: String = key.asInstanceOf[Tuple1[String]].f0

      val count: Long = input.iterator.next()

      out.collect(UrlViewCount(url,window.getEnd,count))

    }
  }


  class MyAggregateFunction extends AggregateFunction[ApacheLogEvent,Long,Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

}
