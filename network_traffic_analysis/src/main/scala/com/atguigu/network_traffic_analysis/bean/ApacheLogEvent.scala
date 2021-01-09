package com.atguigu.network_traffic_analysis.bean

case class ApacheLogEvent (ip: String,
                           userId: String,
                           eventTime: Long,
                           method: String,
                           url: String)
