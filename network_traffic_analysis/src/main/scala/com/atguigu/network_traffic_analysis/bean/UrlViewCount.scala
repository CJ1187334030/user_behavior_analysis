package com.atguigu.network_traffic_analysis.bean

case class UrlViewCount(url: String,
                        windowEnd: Long,
                        count: Long)