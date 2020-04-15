package com.wdk.mr.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/4/15 0015 23:17
 * @Version: v1.0
 **/

public class Step1IndexDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length == 0){
            args=new String[]{"D:\\input\\index","D:\\output\\index"};
        }

        Configuration conf = new Configuration();

        //获取Job实例
        Job job = Job.getInstance(conf);

        //设置jar路径
        job.setJarByClass(Step1IndexDriver.class);

        //设置Mapper Reducer
        job.setMapperClass(Step1IndexMapper.class);
        job.setReducerClass(Step1IndexReducer.class);

        //设置Map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置最终输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交
        boolean flag = job.waitForCompletion(true);

        System.exit(flag?0:1);
    }
}
