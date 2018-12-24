package com.utils

/**
  * 处理地域维度指标工具类
  */
object LocationUtils {
  // 处理请求数
  def requestUtils(reqMode: Int, prcMode: Int): List[Double] = {
    if (reqMode == 1 && prcMode == 1) {
      List[Double](1, 0, 0)
    } else if (reqMode == 1 && prcMode == 2) {
      List[Double](1, 1, 0)
    } else if (reqMode == 1 && prcMode == 3) {
      List[Double](1, 1, 1)
    } else {
      List[Double](0, 0, 0)
    }
  }

  // 处理竞价和广告成本、消费
  def requestAD(fective: Int, bill: Int, bid: Int, win: Int, ad: Int, winprice: Double, adPayment: Double): List[Double] = {
    if (fective == 1 && bill == 1 && bid == 1) {
      List[Double](1, 0, 0, 0)
    } else if (fective == 1 && bill == 1 && win == 1) {
      if (ad != 0) {
        List[Double](0, 1, win / 1000, adPayment / 1000)
      } else {
        List[Double](0, 0, win / 1000, adPayment / 1000)
      }
    } else {
      List[Double](0, 0, 0, 0)
    }
  }

  // 展示、点击
  def requestShow(reqMode: Int, fective: Int): List[Double] = {
    if (reqMode == 2 && fective == 1) {
      List[Double](1, 0)
    } else if (reqMode == 3 && fective == 1) {
      List[Double](0, 1)
    } else {
      List[Double](0, 0)
    }
  }
}
