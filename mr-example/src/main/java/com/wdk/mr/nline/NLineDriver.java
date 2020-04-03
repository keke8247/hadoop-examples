package com.wdk.mr.nline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/2 16:31
 * @Since version 1.0.0
 */
public class NLineDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length ==0 ){
            args = new String[]{"E:\\input\\nline","E:\\output\\nline_out"};
        }

        Configuration conf = new Configuration();

        //获取job实例
        Job job = Job.getInstance(conf);

        //设置jar路径
        job.setJarByClass(NLineDriver.class);

        //关联Mapper Reducer
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        NLineInputFormat.setNumLinesPerSplit(job,2);
        job.setInputFormatClass(NLineInputFormat.class);


        // 设置Mapper 输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交任务
        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);

    }

}
