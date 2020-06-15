package com.wdk.hive.gulivideo;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020-06-06 9:18
 * @Version: v1.0
 **/

public class VideoETLMapper extends Mapper<Object,Text,NullWritable,Text> {
    Text v = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String etlString = ETLUtil.oriString2ETLString(value.toString());

        if(StringUtils.isBlank(etlString)){
            return;
        }else{
            v.set(etlString);
            context.write(NullWritable.get(),v);
        }
    }
}
