package com.wdk.hbase.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020-05-03 18:00
 * @Version: v1.0
 **/

public class FruitReducer extends TableReducer<NullWritable,Put,NullWritable> {

    @Override
    protected void reduce(NullWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put value : values) {
            context.write(NullWritable.get(),value);
        }
    }
}
