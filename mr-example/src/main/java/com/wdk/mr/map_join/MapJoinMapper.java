package com.wdk.mr.map_join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/4/13 0013 21:43
 * @Version: v1.0
 **/

public class MapJoinMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    Map<String,String> cacheMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取缓寸的小表 写入到内存中

        //获取缓存文件的路径
        URI uri = context.getCacheFiles()[0];

        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(uri.getPath()),"UTF-8"));

        //读取小表内容 写入到Map
        String line ;
        while (StringUtils.isNotEmpty(line = br.readLine())){
            //01	小米
            String[] fields = line.split("\t");
            cacheMap.put(fields[0],fields[1]);
        }

        //关闭流
        IOUtils.closeStream(br);
    }

    Text k = new Text();
    String pName;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //map方法 处理Order.txt 的数据 切割后 和 缓存的数据做拼接
        //1001	01	1
        String line = value.toString();

        String[] fields = line.split("\t");

        pName = cacheMap.get(fields[1]);

        k.set(line+"\t"+pName);

        context.write(k,NullWritable.get());
    }
}
