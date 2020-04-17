package com.wdk.mr.reduce_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/4/13 0013 20:47
 * @Version: v1.0
 **/

public class ReduceJoinMapper extends Mapper<LongWritable,Text,Text,TableBean> {

    String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //需要知道 当前切片是mapTask处理的是哪一个切片 获取该切片的文件名称 作为标记位
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    TableBean tableBean = new TableBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();

        //把order.txt 和 pd.txt 的相同列Pid 作为key,归入到同一个reduce处理 进行join操作.
        //判断当前切片属于哪一个文件
        if(fileName.startsWith("order")){
            //1001	01	1

            //封装TableBean
            String[] fields = line.split("\t");
            tableBean.setId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.valueOf(fields[2]));
            tableBean.setFlag("order");
            tableBean.setpName("");

            //封装key
            k.set(fields[1]);
        }else {
            //01	小米
            String [] fields = line.split("\t");
            tableBean.setId("");
            tableBean.setPid(fields[0]);
            tableBean.setAmount(0);
            tableBean.setFlag("pd");
            tableBean.setpName(fields[1]);

            k.set(fields[0]);
        }

        context.write(k,tableBean);
    }
}
