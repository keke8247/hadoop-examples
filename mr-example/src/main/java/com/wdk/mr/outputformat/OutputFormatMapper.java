package com.wdk.mr.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/4/12 0012 18:42
 * @Version: v1.0
 **/
//http://www.baidu.com
public class OutputFormatMapper extends Mapper<LongWritable,Text,Text,NullWritable>{

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取行数据
        String line = value.toString();

        k.set(line);

        context.write(k,NullWritable.get());
    }
}
