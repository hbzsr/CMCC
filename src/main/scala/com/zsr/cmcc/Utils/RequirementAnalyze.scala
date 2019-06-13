package com.zsr.cmcc.Utils

import org.apache.spark.rdd.RDD

object RequirementAnalyze {

  /**
    * 指标一
    *
    * @param lines
    */
  def requirement01(lines: RDD[(String, List[Double])]): Unit = {
    println("~~~~~~~~~DRIVER~~~~~~~")
    // lines：（日期， List）
    lines.foreachPartition(part => {
      //      val jedis = JedisConnectionPool.getConntection()
      // (数字1用于统计充值订单量，充值金额，充值成功数，充值时长，充值成功数且金额不等于0)
      part.foreach(t => {
        //        // 充值订单数
        //        jedis.hincrBy(t._1, "total", t._2(0).toLong)
        //        // 充值总金额
        //        jedis.hincrByFloat(t._1, "money", t._2(1))
        //        // 充值成功数
        //        jedis.hincrBy(t._1, "success", t._2(2).toLong)
        //        // 充值总时长
        //        jedis.hincrBy(t._1, "time", t._2(3).toLong)
        println("充值订单数：" + t._2(0).toLong,
          "充值总金额：" + t._2(1),
          "充值成功数：" + t._2(2).toLong,
          "充值总时长：" + t._2(3).toLong)
      })
      //      jedis.close()
    })
  }


}
