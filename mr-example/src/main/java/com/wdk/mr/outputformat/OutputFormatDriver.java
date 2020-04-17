package com.wdk.mr.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/4/12 0012 18:46
 * @Version: v1.0
 **/

public class OutputFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length == 0){
            args=new String[]{"D:\\input\\outputformat","D:\\output\\outputformat"};
        }

        Configuration conf = new Configuration();

        //1 创建Job实例
        Job job = Job.getInstance(conf);

        //2 设置jar路径
        job.setJarByClass(OutputFormatDriver.class);

        //3 关联Map Reduce 类
        job.setMapperClass(OutputFormatMapper.class);
        job.setReducerClass(OutputFormatReducer.class);

        //4 设置MapTask的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输出的OutputFormat
        job.setOutputFormatClass(MyOutputFormat.class);

        //6 设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7 提交作业
        boolean flag = job.waitForCompletion(true);

        System.exit(flag?0:1);
    }
}
