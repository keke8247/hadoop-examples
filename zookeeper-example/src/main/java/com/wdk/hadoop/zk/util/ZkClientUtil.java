package com.wdk.hadoop.zk.util;

import com.wdk.hadoop.common.util.PropertiesUtil;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @Description:
 *  zookeeper 客户端工具类
 *  zkClient = new ZooKeeper(connections,sessionTimout,watcher,sessionId,sessionPasswd,canBeReadOnly);
 *  构造Zk参数说明
 *  connections : zk服务器列表, ip:port,ip:port,ip:port 组成,也可以加上 简历zk连接后访问的根目录 在最后一个 ip:port/path
 *                  这样该客户端所有的操作都是基于这个目录下面的
 *  sessionTimout: 会话超时时间,毫秒为单位.在会话周期内通过心跳机制保持活性,一旦在会话超时时间内没有有效的心跳检测,该会话失效
 *  watcher: watch事件监听器.实现了org.apache.zookeeper.Watcher接口的类对象
 *  sessionId/sessionPassword : 会话ID和秘钥,用于实现会话复活.这两个值可以通过第一次建立会话之后 getSessionId()和getSessionPasswd()获得.
 *  canBeReadOnly : zk集群中一旦某个机器和过半以上的机器失去连接,那么该服务器将不提供服务.但是某些场景下我们希望提供只读服务.可以通过该参数控制.
 * @Author:wang_dk
 * @Date:2020/2/9 0009 12:53
 * @Version: v1.0
 **/

public class ZkClientUtil implements Watcher{
    private static final Logger logger = LoggerFactory.getLogger(ZkClientUtil.class);

    private static CountDownLatch cd = new CountDownLatch(1);

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
            zkClient = new ZooKeeper(connections,SESSION_TIMEOUT,new ZkClientUtil());
            cd.await(); //阻塞当前线程 直到收到连接成功事件通知.
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
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

    @Override
    public void process(WatchedEvent event) {
        //new Zookeeper()直接返回的对象应用 并没有连接完成, zk连接创建完成会发送一个事件通知给到客户端.状态为syncConnected
        if(Event.KeeperState.SyncConnected == event.getState()){
            cd.countDown();
        }
    }
}
