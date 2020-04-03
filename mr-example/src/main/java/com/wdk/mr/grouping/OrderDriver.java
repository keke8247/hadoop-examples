package com.wdk.mr.grouping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/3 18:10
 * @Since version 1.0.0
 */
public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length == 0){
            args = new String[]{"E:\\input\\groupingComparator","E:\\output\\groupingComparator_out"};
        }

        Configuration conf = new Configuration();
        //获取Job实例
        Job job = Job.getInstance(conf);

        //设置jar路径
        job.setJarByClass(OrderDriver.class);

        //关联Mapper Reducer
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        //关联分组排序类
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        //设置Map输出类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置总的输出
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交任务
        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);
    }

}
