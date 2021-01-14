package com.atguigu.order_timeout_detect.bean

case class OrderEvent( orderId: Long,
                       eventType: String,
                       eventTime: Long)
