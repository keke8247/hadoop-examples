package com.wdk.hadoop.kafka.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;


/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/2/19 12:14
 * @Since version 1.0.0
 */
public class KafkaConsumerFactory {
    public static Consumer getConsumer(){
        Consumer consumer = new KafkaConsumer(KafkaProperties.getConsumerProperties());
        return consumer;
    }
}
