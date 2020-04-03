package com.wdk.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/3 16:55
 * @Since version 1.0.0
 */
public class KeySortPartitioner extends Partitioner<FlowBean,Text>{
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        String startStr = text.toString().substring(0,3);

        int partitions = 4;

        if("136".equals(startStr)){
            partitions = 0;
        }else if("137".equals(startStr)){
            partitions = 1;
        }else if("138".equals(startStr)){
            partitions = 2;
        }else if("139".equals(startStr)){
            partitions = 3;
        }
        return partitions;
    }
}
