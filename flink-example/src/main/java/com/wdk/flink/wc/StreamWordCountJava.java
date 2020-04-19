package com.wdk.flink.wc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020-04-19 9:19
 * @Version: v1.0
 **/

public class StreamWordCountJava {
    public static void main(String[] args) throws Exception {
        //构建Flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

//        DataSource<String> dataSource = env.readTextFile("D:\\files\\program\\idea\\hadoop-examples\\flink-example\\src\\main\\resources\\test.txt");

        DataStreamSource<String> dataSource = env.socketTextStream("master", 7777);

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = value.toLowerCase().split(" ");

                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }
            }
        }).filter(item -> item.f0.contains("h"))
                .keyBy(0);

        //开窗
//        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(5));

        //两种方式都可以统计出wordcount
        //1.使用reduce
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyedStream.reduce((tmp, tmp2) -> new Tuple2(tmp.f0, tmp.f1 + tmp2.f1));
        reduce.print().setParallelism(1);

        //2.使用sum
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
        sum.print().setParallelism(1);

        env.execute("flink word count for java ");
    }
}
