package com.wdk.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/2 10:32
 * @Since version 1.0.0
 */
public class FlowSumMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    Text k = new Text();
    FlowBean flowBean = new FlowBean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] splits = line.split(" ");

        k.set(splits[0]);

        long upFlow = Long.valueOf(splits[1]);
        Long downFlow = Long.valueOf(splits[2]);
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);

        context.write(k,flowBean);
    }
}
