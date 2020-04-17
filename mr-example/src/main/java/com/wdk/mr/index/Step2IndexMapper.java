package com.wdk.mr.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/4/15 0015 23:24
 * @Version: v1.0
 **/

public class Step2IndexMapper extends Mapper<LongWritable,Text,Text,Text>{

    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();

        //切割数据
        String[] fields = line.split("--");

        //组装key
        k.set(fields[0]);
        //组装Value
        v.set(fields[1].replace(" ","-->"));

        context.write(k,v);
    }
}
