package com.wdk.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/3 16:01
 * @Since version 1.0.0
 */
public class KeySortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length == 0){
            args = new String[]{"E:\\output\\flowsum_out","E:\\output\\keysort_out"};
        }

        Configuration conf = new Configuration();
        //获取job实例
        Job job = Job.getInstance(conf);

        //设置jar路径
        job.setJarByClass(KeySortDriver.class);

        //关联Mapper Reducer
        job.setMapperClass(KeySortMapper.class);
        job.setReducerClass(KeySortReducer.class);

        //设置Map的输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //关联分区器 设置分区数
        job.setPartitionerClass(KeySortPartitioner.class);
        job.setNumReduceTasks(5);

        //设置最终输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交任务
        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);

    }
}
