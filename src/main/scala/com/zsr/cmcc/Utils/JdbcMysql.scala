package com.zsr.cmcc.Utils

import java.sql.{Connection, DriverManager}
import java.util

import com.typesafe.config.ConfigFactory

/**
  * MySQL 连接池
  */
object JdbcMysql {
  private val max_connection = 10 // 连接池总数
  private val connection_num = 10 // 产生连接数
  private var current_num = 0 // 当前连接池已产生的连接数
  private val pools = new util.LinkedList[Connection]() // 连接池
  private val load = ConfigFactory.load() // jdbc conf
  /**
    * 加载驱动
    */
  private def before() {
    if (current_num >= max_connection && !pools.isEmpty()) {
      print("busyness")
      Thread.sleep(2000)
      before()
    } else {
      Class.forName(load.getString("db.default.driver"))
    }
  }

  /**
    * 获得连接
    */
  private def initConn(): Connection = {
    val conn = DriverManager.getConnection(
      load.getString("db.default.url"),
      load.getString("db.default.user"),
      load.getString("db.default.password")
    )
    conn
  }

  /**
    * 初始化连接池
    */
  private def initConnectionPool(): util.LinkedList[Connection] = {
    AnyRef.synchronized({
      if (pools.isEmpty()) {
        before()
        for (i <- 1 to connection_num.toInt) {
          pools.push(initConn())
          current_num += 1
        }
      }
      pools
    })
  }

  /**
    * 获得连接
    */
  def getConn(): Connection = {
    initConnectionPool()
    pools.poll()
  }

  /**
    * 释放连接
    */
  def releaseCon(con: Connection) {
    pools.push(con)
  }
}
