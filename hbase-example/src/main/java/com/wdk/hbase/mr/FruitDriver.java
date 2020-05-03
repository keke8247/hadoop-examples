package com.wdk.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020-05-03 18:11
 * @Version: v1.0
 **/

public class FruitDriver implements Tool {
    private static Configuration conf;

    @Override
    public int run(String[] strings) throws Exception {
        //创建Job实例
        Job job = Job.getInstance(conf);

        //设置驱动类路径
        job.setJarByClass(FruitDriver.class);

        //设置Mapper类
        job.setMapperClass(FruitMapper.class);

        //设置Mapper输出类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置Mapper 输入
        FileInputFormat.setInputPaths(job, new Path(strings[0]));

        //设置Reducer
        TableMapReduceUtil.initTableReducerJob(strings[1], FruitReducer.class, job);

        boolean result = job.waitForCompletion(true);


        return result ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;

    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        conf = HBaseConfiguration.create();
        if(args.length == 0){
            args = new String[]{"D:\\files\\program\\idea\\hadoop-examples\\hbase-example\\src\\main\\resources\\fruit.txt","wdk_hbase:fruit"};
        }
        ToolRunner.run(new FruitDriver(),args);
    }
}
