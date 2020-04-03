package com.wdk.mr.grouping;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/3 18:06
 * @Since version 1.0.0
 */
public class OrderMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {

    OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取行数据
        String line = value.toString();

        //切割数据
        String [] items = line.split("\t");

        //封装key value
        k.setOrderId(Integer.valueOf(items[0]));
        k.setPrice(Double.valueOf(items[2]));

        context.write(k,NullWritable.get());
    }
}
