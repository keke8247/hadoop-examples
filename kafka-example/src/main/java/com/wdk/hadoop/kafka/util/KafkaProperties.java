package com.wdk.hadoop.kafka.util;

import com.wdk.hadoop.common.util.PropertiesUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/2/19 9:54
 * @Since version 1.0.0
 */
public class KafkaProperties {

    private static Properties producerProperties = null;

    private static Properties consumerProperties = null;


    public static Properties getKafkaProducerProperties(){
        if(null == producerProperties){
            synchronized (KafkaProperties.class){
                if(null == producerProperties){
                    producerProperties = new Properties();
                    //kafka 集群host
                    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getValue("kafka.cluster.hostlist","kafka.producer.properties"));

                    //producer ack级别  这里设为all 等待ISR集合所有副本节点应答
                    producerProperties.put(ProducerConfig.ACKS_CONFIG,PropertiesUtil.getValue("kafka.producer.acks","kafka.producer.properties"));

                    //重试次数
                    producerProperties.put(ProducerConfig.RETRIES_CONFIG,PropertiesUtil.getValue("kafka.producer.retries","kafka.producer.properties"));

                    //发送消息批次大小
                    producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG,PropertiesUtil.getValue("kafka.producer.batch.size","kafka.producer.properties"));

                    //请求延迟  和 BATCH_SIZE_CONFIG 两个条件 满足一个 就发送该批次的消息.
                    producerProperties.put(ProducerConfig.LINGER_MS_CONFIG,PropertiesUtil.getValue("kafka.producer.linger.ms","kafka.producer.properties"));

                    //发送缓冲区大小 其实就是 RecordAccumulator大小
                    producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,PropertiesUtil.getValue("kafka.producer.buffer.memory","kafka.producer.properties"));

                    //key序列化器
                    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,PropertiesUtil.getValue("kafka.producer.key.serializer","kafka.producer.properties"));

                    //value序列化器
                    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,PropertiesUtil.getValue("kafka.producer.value.serializer","kafka.producer.properties"));

                    // ...还有很多配置 可以参考ProducerConfig里面的进行按需配置.

                    //可以添加自定义的分区器
//                    producerProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.wdk.hadoop.kafka.util.PartitionerUtil");
                }
            }
        }
        return producerProperties;
    }

    public static Properties getConsumerProperties(){
        if(null == consumerProperties){
            synchronized (KafkaProperties.class){
                if(null == consumerProperties){
                    consumerProperties = new Properties();
                    //kafka 集群host
                    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getValue("kafka.cluster.hostlist","kafka.consumer.properties"));

                    //开启自动提交
                    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, PropertiesUtil.getValue("kafka.consumer.enable.auto.commit","kafka.consumer.properties"));

                    //自动提交延迟时间
                    consumerProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, PropertiesUtil.getValue("kafka.consumer.auto.commit.interval.ms","kafka.consumer.properties"));

                    // key,value 反序列化器
                    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,PropertiesUtil.getValue("kafka.consumer.key.deserializer","kafka.consumer.properties"));

                    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,PropertiesUtil.getValue("kafka.consumer.value.deserializer","kafka.consumer.properties"));

                    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,PropertiesUtil.getValue("kafka.consumer.group.id","kafka.consumer.properties"));
                }
            }
        }
        return consumerProperties;
    }
}
