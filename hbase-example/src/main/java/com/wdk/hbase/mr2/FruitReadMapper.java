package com.wdk.hbase.mr2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * 将Hbase 表中的数据 清洗之后写入另外一张Hbase表
 * @Author:wang_dk
 * @Date:2020-05-03 19:57
 * @Version: v1.0
 **/

public class FruitReadMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Cell[] cells = value.rawCells();

        Put put = new Put(key.copyBytes());

        for (Cell cell : cells) {
            if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){ //只保留 columnQualifier 为 name的数据
                    put.add(cell);
                }else{
                    continue;
                }
            }
        }

        context.write(key,put);
    }
}
