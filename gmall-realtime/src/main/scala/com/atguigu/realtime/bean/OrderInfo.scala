package com.atguigu.realtime.bean

case class OrderInfo(id: String,
                     province_id: String,
                     var consignee: String,
                     order_comment: String,
                     var consignee_tel: String,
                     order_status: String,
                     payment_way: String,
                     user_id: String,
                     img_url: String,
                     total_amount: Double,
                     expire_time: String,
                     delivery_address: String,
                     create_time: String,
                     operate_time: String,
                     tracking_no: String,
                     parent_order_id: String,
                     out_trade_no: String,
                     trade_body: String,
                     var create_date: String = null,
                     var create_hour: String = null) {
    // "2020-07-17 21:07:56"
    create_date = create_time.substring(0, 10)
    create_hour = create_time.substring(11, 13)
    
    // 对敏感信息, 做脱敏处理
    consignee = consignee.substring(0, 1) + "**"
    // 13838384381
    consignee_tel = consignee_tel.replaceAll("(\\d{3})\\d{4}(\\d{4})", "$1****$2")
}

