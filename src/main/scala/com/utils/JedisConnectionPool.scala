package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Jedis连接池
  */
object JedisConnectionPool {
  // 获取到配合对象
  val config = new JedisPoolConfig()
  // 设置最大链接数
  config.setMaxTotal(20)
  // 设置最大空闲链接数
  config.setMaxIdle(10)
  // 创建jeids连接
  val pool = new JedisPool(config, "mini1", 6379, 20000)

  def getConnection(): Jedis = {
    pool.getResource
  }
}
