package com.wdk.mr.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/4/15 0015 23:08
 * @Version: v1.0
 **/

public class Step1IndexMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    String name;
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取切片文件名
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据  atguigu pingping
        String line = value.toString();

        //切割数据
        String[] fields = line.split(" ");

        for (String field : fields) {
            //组装Key
            k.set(field+"--"+name);
            context.write(k,v);
        }
    }
}


