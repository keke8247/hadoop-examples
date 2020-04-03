package com.wdk.mr.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/3 18:16
 * @Since version 1.0.0
 */
public class OrderGroupingComparator extends WritableComparator {

    //设置要比较的Key类型 和 是否要创建实例
    protected OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    //把 orderId 相同的数据 放到同一个reduce方法执行
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        int result = 0;

        if(aBean.getOrderId() > bBean.getOrderId()){
            result = 1;
        }else if(aBean.getOrderId() < bBean.getOrderId()){
            result = -1;
        }

        return result;
    }
}
