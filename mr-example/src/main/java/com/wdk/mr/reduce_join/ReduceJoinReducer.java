package com.wdk.mr.reduce_join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/4/13 0013 20:57
 * @Version: v1.0
 **/

public class ReduceJoinReducer extends Reducer<Text,TableBean,TableBean,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //处理数据  根据标志位 判断来源于哪一个表 order --> pid 是多对一的关系

        List<TableBean> orders = new ArrayList<>();
        TableBean pdBean = new TableBean();

        for (TableBean tableBean : values) {
            if("order".equals(tableBean.getFlag())){    //来自order.txt
                try {
                    TableBean tmp = new TableBean();
                    BeanUtils.copyProperties(tmp,tableBean);
                    orders.add(tmp);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }else{
                try {
                    BeanUtils.copyProperties(pdBean,tableBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (TableBean tableBean : orders) {
            tableBean.setpName(pdBean.getpName());
            context.write(tableBean,NullWritable.get());
        }

    }
}
