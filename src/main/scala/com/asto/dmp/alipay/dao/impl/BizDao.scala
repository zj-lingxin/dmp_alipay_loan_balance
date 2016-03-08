package com.asto.dmp.alipay.dao.impl

import com.asto.dmp.alipay.base.Constants
import com.asto.dmp.alipay.dao.SQL
import com.asto.dmp.alipay.util.Utils

object BizDao extends scala.Serializable {
  def getDistinctTrade = {
    BaseDao.getAccountTradeProps(SQL().select(Constants.Schema.DISTINCT_TRADE))
      .map(a => (a(0), a(1), a(2), a(3), a(4), a(5), a(6))).distinct()
  }

  /**
   * 输出结果如下：
   * ((2088102736808892,2015-01),24526.06)
   * ((2088102736808892,2015-02),26526.06)
   * @return
   */
  def getOrderLoan = {
    BaseDao.getDistinctTradeProps(SQL().select("owner_user_id,create_time,total_amount")
      .where("in_out_type = 'in' and order_title like '%订单贷%'"))
      .map(a => ((a(0), a(1).substring(0, 7)), a(2).toDouble))
      .aggregateByKey(0D)((a, b) => a + b, (a, b) => a + b)
      .mapValues(v => Utils.retainDecimal(v / 800)) //保留两位小数，应该是除以100后再除以8。100是分转化为元的单位。8是业务需要
      .sortByKey()
  }

  /**
   * 输出结果如下：
   * ((2088102736808892,2015-01),24526.06)
   * ((2088102736808892,2015-02),26526.06)
   * ((2088102736808892,2015-03),25677.8)
   */
  def getOtherLoan(loanName: String) = {
    val inOutRDD = BaseDao.getDistinctTradeProps(SQL().select("owner_user_id,create_time,in_out_type,total_amount").where(s"order_title like '%$loanName%'"))
      .map(a => ((a(0), a(1).substring(0, 10), a(2)), a(3).toDouble))
      .aggregateByKey(0D)((a, b) => a + b, (a, b) => a + b)
      .persist()
    val inRDD = inOutRDD.filter(_._1._3 == "in").map(t => ((t._1._1, t._1._2), t._2))
    val outRDD = inOutRDD.filter(_._1._3 == "out").map(t => ((t._1._1, t._1._2), t._2))

    inRDD.fullOuterJoin(outRDD)
      .map(t => (t._1, (t._2._1.getOrElse(0D), t._2._2.getOrElse(0D))))
      .mapValues(v => v._1 - v._2)
      .map(t => (t._1._1, (t._1._2, t._2)))
      .groupByKey()
      .map(t => handleData(t._1, t._2.toList.sorted))
      .flatMap(t => t)
  }

  private def handleData(userId: String, data: List[(String, Double)]) = {
    //1、累加
    var acc = 0D
    var middleData = data.map { t => acc += t._2; (t._1, acc) }
    //2、如果存在负数，加上负数的绝对值
    val minValue = middleData.map(_._2).min
    if (minValue < 0) middleData = (for (i <- middleData.indices) yield (middleData(i)._1, middleData(i)._2 - minValue)).toList

    //3、如果一个月有多条数据，去除前面几条数据
    val monthData = middleData.map(t => (t._1.substring(0, 7), t._2))
    monthData.indices.map {
      i =>
        if (i + 1 < monthData.length && monthData(i)._1 == monthData(i + 1)._1) (("", ""), 0D)
        else ((userId, monthData(i)._1), Utils.retainDecimal(monthData(i)._2 / 100))
    }.filter(_._1 !=("", ""))
  }

  def getAllLoan = {
    getOrderLoan.cogroup(getOtherLoan("信用贷"), getOtherLoan("大促贷"), getOtherLoan("等额本金"))
      .map(t => (t._1._1, t._1._2,
      getIteratorHead(t._2._1),
      getIteratorHead(t._2._2),
      getIteratorHead(t._2._3),
      getIteratorHead(t._2._4)))
      .map(t => (t._1, t._2, t._3, t._4, t._5, t._6, t._3 + t._4 + t._5 + t._6))
      .sortBy(t => (t._1, t._2))
  }

  private def getIteratorHead(itr: Iterable[Double]): Double = {
    if (itr.nonEmpty) itr.head else 0D
  }
}
