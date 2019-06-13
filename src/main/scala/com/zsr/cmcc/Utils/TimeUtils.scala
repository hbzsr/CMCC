package com.zsr.cmcc.Utils

import java.text.SimpleDateFormat

object TimeUtils {
  def costtime(starttime: String, recivcetime: String): Long = {
    val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    // 20170412065954904504931593353044
    val startTime = sdf.parse(starttime.substring(0, 17)).getTime
    val endTime = sdf.parse(recivcetime).getTime
    endTime - startTime
  }
}
