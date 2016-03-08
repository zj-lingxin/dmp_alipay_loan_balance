package com.asto.dmp.alipay.base

object Constants {

  /** App中的常量与每个项目相关 **/
  object App {
    val NAME = "支付宝贷款月末余额"
    val YEAR_MONTH_DAY_FORMAT = "yyyy-MM-dd"
    val YEAR_MONTH_FORMAT = "yyyy-MM"
    val DIR = s"${Hadoop.DEFAULT_FS}/alipay"
    var TODAY: String = _
    var TIMESTAMP: Long = _
  }

  object Hadoop {
    val JOBTRACKER_ADDRESS = "centos05:9000"
    val DEFAULT_FS = s"hdfs://$JOBTRACKER_ADDRESS"
  }

  /** 输入文件路径 **/
  object InputPath {
    val SEPARATOR = "\t"
    //val SEPARATOR = ","
    private val INPUT_DIR = s"${Hadoop.DEFAULT_FS}/alipay_data/online/trade"
    val ALIPAY_ACCOUNT_TRADE = s"$INPUT_DIR/jsb/*/*"
    //val ALIPAY_ACCOUNT_TRADE = s"$INPUT_DIR/jsb-test/1/*"
  }

  /** 输出文件路径 **/
  object OutputPath {
    val SEPARATOR = "\t"
    private val OUTPUT_DIR = s"${Hadoop.DEFAULT_FS}/alipay_output/offline/text"
    val DISTINCT_TRADE = s"$OUTPUT_DIR/distinct_trade_temp/"
    val RESULT = s"$OUTPUT_DIR/${App.TODAY}/loan_balance/"
  }

  /** 表的模式 **/
  object Schema {
    val ALIPAY_ACCOUNT_TRADE = "alipay_order_no,merchant_order_no,order_type,order_status,owner_user_id,owner_logon_id," +
      "owner_name,opposite_user_id,opposite_name,opposite_logon_id,partner_id,order_title,total_amount,service_charge," +
      "order_from,create_time,modified_time,in_out_type"
    val DISTINCT_TRADE = "owner_user_id,create_time,in_out_type,total_amount,order_title"
  }

}
