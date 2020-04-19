package com.wdk.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020-04-19 9:56
 * @Version: v1.0
 **/

public class WordCountJava {
    public static void main(String[] args) throws Exception {

        //构建Flink运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataSource<String> dataSource = env.readTextFile("D:\\files\\program\\idea\\hadoop-examples\\flink-example\\src\\main\\resources\\test.txt");

        AggregateOperator<Tuple2<String, Integer>> sum = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] arrs = value.split(" ");
                for (String arr : arrs) {
                    out.collect(new Tuple2(arr, 1));
                }
            }
        }).groupBy(0).sum(1);

        sum.print();

        env.execute("flink word count for java");
    }
}
