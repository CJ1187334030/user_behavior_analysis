package com.atguigu.user_behavior_analysis.bean

case class UserBehavior (userId:Long,
                         itemId:Long,
                         categoryId:Int,
                         behavior:String,
                         timestamp:Long)
