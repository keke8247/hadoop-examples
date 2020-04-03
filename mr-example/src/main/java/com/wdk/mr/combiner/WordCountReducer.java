package com.wdk.mr.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/1 17:30
 * @Since version 1.0.0
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

    IntWritable v = new IntWritable();

    /**
     * @Description:
     * reduce方法 每个可以调用一次
     * @Param
     * @return
    */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        v.set(sum);

        context.write(key,v);
    }
}
