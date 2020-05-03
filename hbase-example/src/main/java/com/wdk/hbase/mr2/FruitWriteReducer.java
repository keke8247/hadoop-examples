package com.wdk.hbase.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020-05-03 20:03
 * @Version: v1.0
 **/

public class FruitWriteReducer extends TableReducer<ImmutableBytesWritable,Put,NullWritable> {

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        //遍历values 写出
        for (Put value : values) {
            context.write(NullWritable.get(),value);
        }
    }
}
