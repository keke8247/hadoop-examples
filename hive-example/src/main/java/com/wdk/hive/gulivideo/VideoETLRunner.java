package com.wdk.hive.gulivideo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020-06-06 9:23
 * @Version: v1.0
 **/

public class VideoETLRunner implements Tool {
    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        conf = this.getConf();
        conf.set("inpath",args[0]);
        conf.set("outpath",args[1]);

        //获取job实例
        Job job = Job.getInstance(conf);

        //设置jar路径
        job.setJarByClass(VideoETLRunner.class);

        //设置Mapper类路径
        job.setMapperClass(VideoETLMapper.class);

        //设置Mapper输出类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置reduce的个数为0  只是清洗数据 不需要reduce方法
        job.setNumReduceTasks(0);

        //设置输入输出路径
        this.initJobInputPath(job);
        this.initJobOutputPath(job);

        return 0;
    }

    /**
     * @Description:
     * 设置输出路径.如果已经存在 删除
     * @Date 2020-06-06 9:32
     * @Param
     * @return
     **/
    private void initJobOutputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String outPathString = conf.get("outpath");
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(outPathString);
        if(fs.exists(outPath)){
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
    }

    /**
     * @Description:
     * 校验输入路径合法性.并设置输入路径.
     * @Date 2020-06-06 9:33
     * @Param
     * @return
     **/
    private void initJobInputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String inPathString = conf.get("inpath");
        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path(inPathString);
        if(fs.exists(inPath)){
            FileInputFormat.addInputPath(job, inPath);
        }else{
            throw new RuntimeException("HDFS 中该文件目录不存在： " +
                    inPathString);
        }
    }

    public static void main(String[] args) {
        try {
            int resultCode = ToolRunner.run(new VideoETLRunner(),args);
            if(resultCode == 0){
                System.out.println("Success!");
            }else{
                System.out.println("Fail");
            }
            System.exit(resultCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
