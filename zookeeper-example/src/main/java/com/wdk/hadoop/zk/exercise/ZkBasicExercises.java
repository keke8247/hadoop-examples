package com.wdk.hadoop.zk.exercise;

import com.wdk.hadoop.zk.util.ZkClientUtil;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Description:
 *  zk操作基本练习
 * @Author:wang_dk
 * @Date:2020/2/9 0009 13:31
 * @Version: v1.0
 **/

public class ZkBasicExercises implements Watcher{
    private static final Logger logger = LoggerFactory.getLogger(ZkBasicExercises.class);

    ZooKeeper zk = null;

    //创建节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        logger.info("创建节点:{}","testNodeFromJava");
        zk = ZkClientUtil.getZkClient();
        zk.create("/testNodeFromJava","testNodeFromJava".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    //异步创建一个节点
    @Test
    public void syncCreateNode() throws InterruptedException {
        logger.info("异步创建一个节点:{}","syncNode");
        zk = ZkClientUtil.getZkClient();
        zk.create("/syncNode", "syncNode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                System.out.println(name);
            }
        },this);
        TimeUnit.SECONDS.sleep(1000);
    }

    @Test
    public void readPath() throws KeeperException, InterruptedException {
        zk = ZkClientUtil.getZkClient();
        String ret = new String(zk.getData("/testNodeFromJava",false,null));
        logger.info("读取节点:{}:{}","testNodeFromJava",ret);
    }

    @Test
    public void setData() throws KeeperException, InterruptedException {
        //version = -1 时候 匹配所有版本
        zk = ZkClientUtil.getZkClient();
        zk.setData("/testNodeFromJava","setDataTest".getBytes(),-1);
        logger.info("/testNodeFromJava修改后的数据:{}",new String(zk.getData("/testNodeFromJava",false,null)));
    }

    @Test
    public void deleteNode() throws KeeperException, InterruptedException {
        createNode();
        zk = ZkClientUtil.getZkClient();

        //获取 子节点  并且监控子节点的变动  如果子节点增加或者修改或者删除 都会触发 process方法
        List<String> lists = zk.getChildren("/",this);

        zk.delete("/testNodeFromJava",-1);

        TimeUnit.SECONDS.sleep(4000);
    }

    @Test
    public void isExist() throws KeeperException, InterruptedException {
        zk = ZkClientUtil.getZkClient();
        logger.info("/testNodeFromJava message:{}",zk.exists("/testNodeFromJava",this));
        TimeUnit.SECONDS.sleep(4000);
    }

    @Override
    public void process(WatchedEvent event) {
        //监控事件被触发 回调这个方法 可以在该方法里面加入业务逻辑
        logger.info("~~~~~~~~~~~~~~~~~这是Watcher信息~~~~~~~~~~~~~~~~~~~~~~~state:{},eventName:{},path:{}",new Object[]{event.getState(),event.getType(),event.getPath()});

        if(Event.EventType.NodeChildrenChanged == event.getType()){ //子节点列表数据发生变更
            try {
                zk.getChildren("/",this).forEach(node -> {
                    System.out.println("watcher process :"+ node);
                });
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }else if (Event.EventType.NodeCreated == event.getType() ||
                Event.EventType.NodeDataChanged == event.getType()||
                Event.EventType.NodeDeleted == event.getType()){
            try {
                logger.info("node exists :"+ zk.exists("/testNodeFromJava",this));
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}
