package com.asto.dmp.alipay.service

import com.asto.dmp.alipay.base.Contexts
import org.apache.spark.Logging

trait Service extends Logging with scala.Serializable {
  protected val sqlContext = Contexts.sqlContext
  protected var errorHead: String = s"${getClass.getSimpleName}的run()方法出现异常"

  protected def handlingExceptions(t: Throwable) {
    logError(errorHead, t)
  }

  protected def printStartLog() = logInfo(s"开始运行${getClass.getSimpleName}的run()方法")

  protected def printEndLog() = logInfo(s"${getClass.getSimpleName}的run()方法运行结束")

  protected def runServices()

  def run() {
    try {
      printStartLog()
      runServices()
    } catch {
      case t: Throwable => handlingExceptions(t)
    } finally {
      printEndLog()
    }
  }
}
