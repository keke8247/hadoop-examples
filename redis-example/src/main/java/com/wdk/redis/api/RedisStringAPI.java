package com.wdk.redis.api;

import com.wdk.redis.connection.JedisPoolFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @Description:
 *  Redis String test
 * @Author:wang_dk
 * @Date:2020/3/17 0017 11:58
 * @Version: v1.0
 **/

public class RedisStringAPI {
    public static void main(String[] args) {
        JedisPool jedisPool = JedisPoolFactory.getJedisPool();
        Jedis jedis = jedisPool.getResource();

        try {
            System.out.println(jedis.ping());
        }catch (Exception e){

        }finally {
            JedisPoolFactory.freedResource(jedis);
        }
    }
}
