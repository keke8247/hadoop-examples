package com.wdk.hadoop.kafka.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Description
 * 构建自己的partition分区器
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/2/19 11:37
 * @Since version 1.0.0
 */
public class PartitionerUtil implements Partitioner{
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //里面可以根据自己的业务 实现分区逻辑.

        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
