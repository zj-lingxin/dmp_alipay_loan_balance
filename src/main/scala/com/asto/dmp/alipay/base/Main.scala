package com.asto.dmp.alipay.base

import com.asto.dmp.alipay.service.ServiceImpl
import com.asto.dmp.alipay.util._

import org.apache.spark.Logging

object Main extends Logging {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    if (argsIsIllegal(args)) return
    handleArgs(args)
    runServices()
    closeResources()
    printEndLogs(startTime)
  }

  def handleArgs(args: Array[String])  = {
    Constants.App.TODAY = DateUtils.timestampToStr(System.currentTimeMillis(), "yyyyMM")
    Constants.Hadoop.JOBTRACKER_ADDRESS = args(0)
    Constants.Hadoop.DEFAULT_FS = s"hdfs://${args(0)}"
    Constants.InputPath.ALIPAY_ACCOUNT_TRADE = s"${Constants.InputPath.INPUT_DIR}/jsb/*${args(1)}/*"
    Constants.OutputPath.RESULT = s"${Constants.OutputPath.OUTPUT_DIR}/${Constants.App.TODAY}/loan_balance/result${args(1)}"
  }

  private def runServices() {
    new ServiceImpl().run()
  }

  /**
   * 关闭用到的资源
   */
  private def closeResources() = {
    Contexts.stopSparkContext()
    //MQAgent.close()
  }

  /**
   * 判断传入的参数是否合法
   */
  private def argsIsIllegal(args: Array[String]) = {
    if (Option(args).isEmpty || args.length < 1 ) {
      logError("请传入程序参数:时间戳")
      true
    } else {
      false
    }
  }

  /**
   * 打印程序运行的时间
   */
  private def printRunningTime(startTime: Long) {
    logInfo(s"程序共运行${(System.currentTimeMillis() - startTime) / 1000}秒")
  }


  /**
   * 最后打印出一些提示日志
   */
  private def printEndLogs(startTime: Long): Unit = {
    printRunningTime(startTime: Long)
  }

}