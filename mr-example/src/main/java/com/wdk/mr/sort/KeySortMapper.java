package com.wdk.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/3 15:55
 * @Since version 1.0.0
 */
public class KeySortMapper extends Mapper<LongWritable,Text,FlowBean,Text> {

    FlowBean k = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取 行数据
        String line = value.toString();

        //切割数据
        String [] items = line.split("\t");

        //组装 key value
        long upFlow = Long.valueOf(items[1]);
        long downFlow = Long.valueOf(items[2]);
        long sumFlow = Long.valueOf(items[3]);
        k.setUpFlow(upFlow);
        k.setDownFlow(downFlow);
        k.setSumFlow(sumFlow);

        String phoneNum = items[0].trim();
        v.set(phoneNum);

        context.write(k,v);
    }
}
