package com.atguigu.login_fail_detect.bean

case class LoginEvent (userId: Long,
                       ip: String,
                       eventType: String,
                       eventTime: Long)
