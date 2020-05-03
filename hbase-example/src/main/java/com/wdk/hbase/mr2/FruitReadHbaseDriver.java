package com.wdk.hbase.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020-05-03 20:04
 * @Version: v1.0
 **/

public class FruitReadHbaseDriver implements Tool {
    private Configuration configuration;

    @Override
    public int run(String[] strings) throws Exception {
        //构建Job实例
        Job job = Job.getInstance(configuration);

        //设置Driver jar路径
        job.setJarByClass(FruitReadHbaseDriver.class);

        //设置Mapper类
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("wdk_hbase:fruit"),
                new Scan(),
                FruitReadMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);

        //设置Reducer类
        TableMapReduceUtil.initTableReducerJob("wdk_hbase:fruit2", FruitWriteReducer.class, job);

        //启动
        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        ToolRunner.run(new FruitReadHbaseDriver(),args);
    }
}
