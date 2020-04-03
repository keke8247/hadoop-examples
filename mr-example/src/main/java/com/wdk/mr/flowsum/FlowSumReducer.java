package com.wdk.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/2 10:35
 * @Since version 1.0.0
 */
public class FlowSumReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    Text k = new Text();
    FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_downFlow = 0;
        for (FlowBean value : values) {
            sum_upFlow += value.getUpFlow();
            sum_downFlow += value.getDownFlow();
        }

        flowBean.setUpFlow(sum_upFlow);
        flowBean.setDownFlow(sum_downFlow);
        flowBean.setSumFlow(sum_upFlow+sum_downFlow);

        k.set(key);
        context.write(k,flowBean);
    }
}
