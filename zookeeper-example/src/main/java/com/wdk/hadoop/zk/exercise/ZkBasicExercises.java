package com.wdk.hadoop.zk.exercise;

import com.wdk.hadoop.zk.util.ZkClientUtil;
import org.apache.zookeeper.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Description:
 *  zk操作基本练习
 * @Author:wang_dk
 * @Date:2020/2/9 0009 13:31
 * @Version: v1.0
 **/

public class ZkBasicExercises implements Watcher{
    private static final Logger logger = LoggerFactory.getLogger(ZkBasicExercises.class);

    //创建节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        logger.info("创建节点:{}","testNodeFromJava");
        ZkClientUtil.getZkClient().create("/testNodeFromJava","testNodeFromJava".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void readPath() throws KeeperException, InterruptedException {
        String ret = new String(ZkClientUtil.getZkClient().getData("/testNodeFromJava",false,null));
        logger.info("读取节点:{}:{}","testNodeFromJava",ret);
    }

    @Test
    public void setData() throws KeeperException, InterruptedException {
        //version = -1 时候 匹配所有版本
        ZooKeeper zooKeeper = ZkClientUtil.getZkClient();
        zooKeeper.setData("/testNodeFromJava","setDataTest".getBytes(),-1);
        logger.info("/testNodeFromJava修改后的数据:{}",new String(zooKeeper.getData("/testNodeFromJava",false,null)));
    }

    @Test
    public void deleteNode() throws KeeperException, InterruptedException {
        createNode();
        ZooKeeper zooKeeper = ZkClientUtil.getZkClient();

        //获取 子节点  并且监控子节点的变动  如果子节点增加或者修改或者删除 都会触发 process方法
        List<String> lists = zooKeeper.getChildren("/",this);

        zooKeeper.delete("/testNodeFromJava",-1);

        lists.forEach(node ->{
            System.out.println(node);
        });
    }

    @Test
    public void isExist() throws KeeperException, InterruptedException {
        ZooKeeper zooKeeper = ZkClientUtil.getZkClient();
        logger.info("/testNodeFromJava message:{}",zooKeeper.exists("/testNodeFromJava",false).toString());
    }

    @Override
    public void process(WatchedEvent event) {
        //监控事件被触发 回调这个方法 可以在该方法里面加入业务逻辑
        logger.info("~~~~~~~~~~~~~~~~~这是Watcher信息~~~~~~~~~~~~~~~~~~~~~~~state:{},eventName:{},path:{}",new Object[]{event.getState(),event.getType(),event.getPath()});
    }
}
