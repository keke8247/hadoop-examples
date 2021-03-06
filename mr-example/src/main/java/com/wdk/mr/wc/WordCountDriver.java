package com.wdk.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/1 17:35
 * @Since version 1.0.0
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length == 0){
            args=new String[]{"D:\\input\\wc","D:\\output\\wc"};
        }

        Configuration conf = new Configuration();
        //1 定义job
        Job job = Job.getInstance(conf);

        // 2 设置jar加载路径
        job.setJarByClass(WordCountDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //使用CombineTextInputFormat 处理小文件,把小文件放到一个split切片.
        job.setInputFormatClass(CombineTextInputFormat.class);
        //设置 切片大小为4M 会把小文件合并到一个split里面
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);

        // 7 提交
//        job.submit();

        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);
    }
}
