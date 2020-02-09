package com.wdk.hadoop.zk.util;

import com.wdk.hadoop.common.util.PropertiesUtil;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Description:
 *  zookeeper 客户端工具类
 * @Author:wang_dk
 * @Date:2020/2/9 0009 12:53
 * @Version: v1.0
 **/

public class ZkClientUtil {
    private static final Logger logger = LoggerFactory.getLogger(ZkClientUtil.class);

    //zk连接超时默认时间
    private static final int SESSION_TIMEOUT = 10000;

    private static String connections;

    private static ZooKeeper zkClient = null;

    static {
        connections = PropertiesUtil.getValue("zk.cluster.hostlist","zookeeper.properties");
        logger.info("获取zk连接信息:{}",connections);
    }

    /**
     * @Description:
     *  不带任何参数 返回zk连接客户端. 默认超时时间为10秒  watcher事件为null
     * @Date 2020/2/9 0009 13:12
     * @Param
     * @return
     **/
    public static ZooKeeper getZkClient() {
        try {
            zkClient = new ZooKeeper(connections,SESSION_TIMEOUT,null);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return zkClient;
    }

    /**
     * @Description:
     *  指定超时时间
     * @Date 2020/2/9 0009 13:18
     * @Param
     * @return
     **/
    public static ZooKeeper getZkClient(int sessionTimout){
        try {
            zkClient = new ZooKeeper(connections,sessionTimout,null);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return zkClient;
    }

    /**
     * @Description:
     * 指定超时时间 并 设置Watcher事件
     * @Date 2020/2/9 0009 13:18
     * @Param
     * @return
     **/
    public static ZooKeeper getZkClient(int sessionTimout, Watcher watcher){
        try {
            zkClient = new ZooKeeper(connections,sessionTimout,watcher);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return zkClient;
    }

    /**
     * @Description:
     * 指定超时时间 watcher事件 只读模式(貌似是无主的时候也可以提供访问)
     * @Date 2020/2/9 0009 13:18
     * @Param
     * @return
     **/
    public static ZooKeeper getZkClient(int sessionTimout, Watcher watcher,boolean canBeReadOnly){
        try {
            zkClient = new ZooKeeper(connections,sessionTimout,watcher,canBeReadOnly);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return zkClient;
    }

    /**
     * @Description:
     * 指定超时时间 watcher事件 重新连接时候的sessionId
     * @Date 2020/2/9 0009 13:18
     * @Param
     *  sessionId:重新连接时要使用的特定会话ID
     *  sessionPasswd:会话密码
     * @return
     **/
    public static ZooKeeper getZkClient(int sessionTimout, Watcher watcher,long sessionId, byte[] sessionPasswd){
        try {
            zkClient = new ZooKeeper(connections,sessionTimout,watcher,sessionId,sessionPasswd);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return zkClient;
    }

    /**
     * @Description:
     * 指定超时时间 watcher事件 重新连接时候的sessionId 模式
     * @Date 2020/2/9 0009 13:18
     * @Param
     *  sessionId:重新连接时要使用的特定会话ID
     *  sessionPasswd:会话密码
     *  canBeReadOnly:是否只读模式
     * @return
     **/
    public static ZooKeeper getZkClient(int sessionTimout, Watcher watcher,long sessionId, byte[] sessionPasswd,boolean canBeReadOnly){
        try {
            zkClient = new ZooKeeper(connections,sessionTimout,watcher,sessionId,sessionPasswd,canBeReadOnly);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return zkClient;
    }

    /**
     * @Description:
     * 释放连接
     * @Date 2020/2/9 0009 14:08
     * @Param
     * @return
     **/
    public static void releaseConnection(ZooKeeper zk){
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
