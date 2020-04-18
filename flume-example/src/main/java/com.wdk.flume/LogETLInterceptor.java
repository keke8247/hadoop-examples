package com.wdk.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * @Description
 * 自定义flume拦截器
 * @Author rdkj
 * @CreatTime 2020/4/17 9:10
 * @Since version 1.0.0
 */
public class LogETLInterceptor implements Interceptor {

    //初始化
    @Override
    public void initialize() {

    }

    //单条event处理
    @Override
    public Event intercept(Event event) {
        return null;
    }

    //多条event处理
    @Override
    public List<Event> intercept(List<Event> list) {
        return null;
    }

    //关闭
    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
