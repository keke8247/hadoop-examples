package com.wdk.mr.map_join;

import com.wdk.mr.reduce_join.ReduceJoinMapper;
import com.wdk.mr.reduce_join.ReduceJoinReducer;
import com.wdk.mr.reduce_join.TableBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/4/13 0013 21:04
 * @Version: v1.0
 **/

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        if(args.length == 0){
            args = new String[]{"D:\\input\\map_join","D:\\output\\map_join"};
        }

        Configuration conf = new Configuration();

        //1.获取Job实例
        Job job = Job.getInstance(conf);

        //2.设置jar路径
        job.setJarByClass(MapJoinDriver.class);

        //3.设置Mapper Reducer class
        job.setMapperClass(MapJoinMapper.class);

        //4.设置Mapper输出类型
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(TableBean.class);

        //5.设置最终输出类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        //在MapTask阶段聚合完成 不需要Reduce阶段
        job.setNumReduceTasks(0);

        job.addCacheFile(new URI("file:///D:/input/reduce_join/pd.txt"));

        //6.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //7.提交任务
        boolean flag = job.waitForCompletion(true);
        System.exit(flag?0:1);
    }
}
