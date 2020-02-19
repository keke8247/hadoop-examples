package com.wdk.hadoop.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Description
 * 消息发送完成 统计成功  or 失败的次数
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/2/19 18:44
 * @Since version 1.0.0
 */
public class CountInterceptor implements ProducerInterceptor<String,Object> {

    public int success;
    public int error;

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> record) {
        return record;
    }

    //消息发送完成 调用该方法
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (null != metadata) {
            success += 1;
        }else{
            error += 1;
        }
    }

    //producer资源关闭 调用该方法
    @Override
    public void close() {
        System.out.println("success:"+success);
        System.out.println("error:"+error);
    }


}
