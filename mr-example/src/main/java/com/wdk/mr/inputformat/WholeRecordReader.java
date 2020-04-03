package com.wdk.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/2 17:10
 * @Since version 1.0.0
 */
public class WholeRecordReader extends RecordReader<Text,BytesWritable>{
    FileSplit split;
    Configuration configuration;
    Text k = new Text();
    BytesWritable v = new BytesWritable();

    //设置标志位
    boolean isProgress = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws InterruptedException {
        this.split = (FileSplit) split;
        this.configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress){

            FSDataInputStream fis = null;
            Path path;
            FileSystem fs = null;
            try{
                //获取文件路径
                path = split.getPath();
                //获取FS文件系统
                fs = path.getFileSystem(configuration);

                //获取文件输入流
                fis = fs.open(path);

                //读取数据到buffer
                byte[] buffer = new byte[(int) split.getLength()];
                IOUtils.readFully(fis,buffer,0,buffer.length);

                //把buffer数据写入到v
                v.set(buffer,0,buffer.length);

                //获取文件路径及名称
                String fileName = path.toString();
                //设置key
                k.set(fileName);

                isProgress = false;

                return true;

            }catch (IOException e){
                e.printStackTrace();
            }finally {
                IOUtils.closeStream(fis);
            }
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
