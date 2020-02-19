package com.wdk.hadoop.kafka.exercise;

import com.wdk.hadoop.kafka.util.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/2/19 18:47
 * @Since version 1.0.0
 */
public class InterceptroProducer {
    public static void main(String[] args) {
        Properties properties = KafkaProperties.getKafkaProducerProperties();

        //添加过滤器链
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,new ArrayList<String>(){
            {
                add("com.wdk.hadoop.kafka.interceptor.TimeInterceptor");
                add("com.wdk.hadoop.kafka.interceptor.CountInterceptor");
            }
        });

        Producer producer = new KafkaProducer(properties);

        for (int i=0;i<10;i++){
            producer.send(new ProducerRecord("kafka_0219","is Value"));
        }

        producer.close();
    }
}
