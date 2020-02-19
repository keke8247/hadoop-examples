package com.wdk.hadoop.kafka.exercise;

import com.wdk.hadoop.kafka.util.KafkaProducerFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/2/19 10:34
 * @Since version 1.0.0
 */
public class KafkaProducerExercises {
    public static void main(String[] args) {
        Producer<String,Object> producer = KafkaProducerFactory.getProducer();

        for (int i=0;i<10;i++){
            producer.send(new ProducerRecord<String, Object>("kafka_0219", "~~~testValue__" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (null != metadata){
                        System.out.println("topic:"+metadata.topic()+" partition:"+metadata.partition()+" offset:"+metadata.offset());
                    }
                }
            });
        }

        KafkaProducerFactory.close(producer);
    }
}
