package com.wdk.hadoop.kafka.exercise;

import com.wdk.hadoop.kafka.util.KafkaConsumerFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Arrays;
import java.util.Collections;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/2/19 12:16
 * @Since version 1.0.0
 */
public class KafkaConsumerExercises {
    public static void main(String[] args) {
        Consumer<String,Object> consumer = KafkaConsumerFactory.getConsumer();

        //设置consumer订阅的主题
        consumer.subscribe(Collections.singletonList("kafka_0219"));

        while (true){
            //读取数据 200ms 轮训间隔
            ConsumerRecords<String,Object> consumerRecords = consumer.poll(200);

            consumerRecords.forEach(consumerRecord -> {
                System.out.println("topic:"+consumerRecord.topic()+" partiton:"+consumerRecord.partition()+" key:"+consumerRecord.key()+" value:"+consumerRecord.value());
            });
        }
    }
}
