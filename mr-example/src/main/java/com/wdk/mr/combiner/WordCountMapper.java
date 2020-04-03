package com.wdk.mr.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/1 17:25
 * @Since version 1.0.0
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //逐条读取的数据
        String line = value.toString();

        //切割
        String[] words = line.split(" ");

        //遍历转换输出类型
        for (String word : words) {
            k.set(word);

            context.write(k,v);
        }
    }
}
