package com.wdk.hadoop.kafka.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/2/19 10:27
 * @Since version 1.0.0
 */
public class KafkaProducerFactory{

    public static Producer<String, Object> getProducer(){
        Producer<String,Object> producer = new KafkaProducer<String,Object>(KafkaProperties.getKafkaProducerProperties());
        return producer;
    }

    public static void close(Producer producer){
        producer.close();
    }
}
