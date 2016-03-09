package com.asto.dmp.alipay.dao.impl

import com.asto.dmp.alipay.base._
import com.asto.dmp.alipay.dao.{Dao, SQL}
import org.apache.spark.rdd.RDD

object BaseDao extends Dao {
  def getDistinctTradeProps(sql: SQL = new SQL()): RDD[Array[String]] = getProps(Constants.OutputPath.DISTINCT_TRADE, Constants.Schema.DISTINCT_TRADE, "distinct_trade", sql)
  def getAccountTradeProps(sql: SQL = new SQL()): RDD[Array[String]] = getProps(Constants.InputPath.ALIPAY_ACCOUNT_TRADE, Constants.Schema.ALIPAY_ACCOUNT_TRADE, "alipay_account_trade", sql)
}
