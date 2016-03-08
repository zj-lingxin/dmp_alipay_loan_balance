package com.asto.dmp.alipay.service

import com.asto.dmp.alipay.base.Constants
import com.asto.dmp.alipay.dao.impl.BizDao
import com.asto.dmp.alipay.util.FileUtils

class ServiceImpl extends Service {
  override protected def runServices(): Unit = {
    FileUtils.saveAsTextFile(BizDao.getDistinctTrade, Constants.OutputPath.DISTINCT_TRADE)
    FileUtils.saveAsTextFile(BizDao.getAllLoan, Constants.OutputPath.RESULT)
  }
}
