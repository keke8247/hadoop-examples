package com.wdk.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/2 17:34
 * @Since version 1.0.0
 */
public class SequenceFileReducer extends Reducer<Text,BytesWritable,Text,BytesWritable>{

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key,values.iterator().next());

    }
}
