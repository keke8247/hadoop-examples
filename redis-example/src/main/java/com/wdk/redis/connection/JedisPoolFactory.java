package com.wdk.redis.connection;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/3/17 0017 11:45
 * @Version: v1.0
 **/

public class JedisPoolFactory {
    private static JedisPool jedisPool = null;

    //单例类 私有化构造函数
    private JedisPoolFactory(){
    }

    public static JedisPool getJedisPool(){
        if (null == jedisPool){
            synchronized (JedisPoolFactory.class){
                if (null == jedisPool){
                    GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
                    poolConfig.setMaxTotal(100);    //最大连接数
                    poolConfig.setMaxIdle(32);  //最大空闲连接
                    poolConfig.setMaxWaitMillis(30*1000);   //最大等待时间
                    poolConfig.setTestOnBorrow(true);   //当调用borrow Object方法时，是否进行有效性检查
                    jedisPool = new JedisPool(poolConfig,"127.0.0.1",6379);
                }
            }
        }
        return jedisPool;
    }

    /**
     * @Description:
     *  释放资源
     * @Date 2020/3/17 0017 11:53
     * @Param
     * @return
     **/
    public static void freedResource(Jedis client){
        if(null != client){
            client.close();
        }
    }
}
