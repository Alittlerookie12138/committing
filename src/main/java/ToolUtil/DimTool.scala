package ToolUtil

object DimTool {
  def ysqqRpt(rMode: Int, pNode: Int): List[Double] = {
    if (rMode == 1 && pNode == 3) {
      List[Double](1, 1, 1)
    } else if (rMode == 1 && pNode >= 2) {
      List[Double](1, 1, 0)
    } else if (rMode == 1 && pNode >= 1) {
      List[Double](1, 0, 0)
    } else {
      List[Double](0, 0, 0)
    }
  }

  def jiangjiaRpt(ecTive: Int, Bill: Int, bid: Int, isWin: Int, orderId: Int): List[Double] = {
    if (ecTive == 1 && Bill == 1 && bid == 1 && orderId != 0) {
      List[Double](1, 0)
    } else if (ecTive == 1 && Bill == 1 && isWin == 1) {
      List[Double](1, 1)
    } else {
      List[Double](0, 0)
    }
  }

  // 广告展示数
  def ggzjRtp(rMode: Int, ecTive: Int): List[Double] = {
    if (rMode == 2 && ecTive == 1) {
      List[Double](1, 0)
    } else if (rMode == 3 && ecTive == 1) {
      List[Double](1, 1)
    } else {
      List[Double](0, 0)
    }
  }

  // 媒介展示数
  def mjjRtp(rMode: Int, ecTive: Int, Bill: Int): List[Double] = {
    if (rMode == 2 && ecTive == 1 && Bill == 1) {
      List[Double](1, 0)
    } else if (rMode == 3 && ecTive == 1 && Bill == 1) {
      List[Double](1, 1)
    } else {
      List[Double](0, 0)
    }
  }

  // 广告消费数
  def ggcbRtp(ecTive: Int, Bill: Int, isWin: Int, winPrice: Double, adPatyment: Double): List[Double] = {
    if (ecTive == 1 && Bill == 1 && isWin == 1) {
      List[Double](winPrice * 1.0 / 1000, adPatyment * 1.0 / 1000)
    } else {
      List[Double](0, 0)
    }
  }
}
