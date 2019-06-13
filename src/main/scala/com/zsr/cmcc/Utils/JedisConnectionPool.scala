package com.zsr.cmcc.Utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Jedis连接池
  */
object JedisConnectionPool {
    // 配置信息
    val config = new JedisPoolConfig
    // 最大连接数
    config.setMaxTotal(20)
    // 最大空闲连接数
    config.setMaxIdle(10)
    // 创建连接
    val pool = new JedisPool(config,"node02",6379,10000)

    def getConntection():Jedis ={
      pool.getResource // 获取到连接
    }
}
