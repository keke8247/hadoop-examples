package com.wdk.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/4/15 0015 23:29
 * @Version: v1.0
 **/

public class Step2IndexReducer extends Reducer<Text,Text,Text,Text> {

    Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        atguigu   a.txt	3
//                  b.txt	2
//                  c.txt	2

        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            sb.append(value.toString()).append("\t");
        }

        v.set(sb.toString());

        context.write(key,v);
    }
}
