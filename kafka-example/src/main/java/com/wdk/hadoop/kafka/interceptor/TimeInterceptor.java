package com.wdk.hadoop.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Description
 * 消息过滤器  在发送的消息 value上面添加时间错
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/2/19 18:41
 * @Since version 1.0.0
 */
public class TimeInterceptor implements ProducerInterceptor<String,Object> {
    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> record) {
        //获取  record 的value值
        String value = String.valueOf(record.value());

        //由于ProducerRecord没有提供 属性的get set 方法 不允许改  返回一个新的
        return new ProducerRecord<String, Object>(record.topic(),record.partition(),record.key(),System.currentTimeMillis()+value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }


}
