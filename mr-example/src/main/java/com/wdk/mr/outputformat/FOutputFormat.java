package com.wdk.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Description:
 *  自定义OutputFormat
 *      根据key的不同 输出到不同文件
 * @Author:wang_dk
 * @Date:2020/4/12 0012 18:53
 * @Version: v1.0
 **/

public class FOutputFormat extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream atguiguOut ;
    FSDataOutputStream otherOut;
    public FOutputFormat(TaskAttemptContext job) {
        //获取文件系统
        FileSystem fs = null;
        try {
            fs = FileSystem.get(job.getConfiguration());

            //定义 包含atguigu的输出路径
            atguiguOut = fs.create(new Path("d:/atguigu.txt"));
            otherOut = fs.create(new Path("d:/other.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if(key.toString().contains("atguigu")){
            atguiguOut.writeBytes(key.toString());
        }else{
            otherOut.writeBytes(key.toString());
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

    }
}
