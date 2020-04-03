package com.wdk.mr.grouping;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/3 18:00
 * @Since version 1.0.0
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private int orderId;
    private double price;

    public OrderBean() {
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }

    @Override
    public int compareTo(OrderBean o) {

        //根据ID正序 price 倒叙
        int result = 0;
        if(orderId > o.orderId){
            result = 1;
        }else if(orderId < o.orderId){
            result = -1;
        }else {
            if(price > o.price){
                result = -1;
            }else if(price < o.price){
                result = 1;
            }
        }

        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readInt();
        this.price = in.readDouble();
    }

    /**
     * Gets the value of orderId.
     *
     * @return the value of orderId.
     */
    public int getOrderId() {
        return orderId;
    }

    /**
     * Sets the orderId.
     * <p>
     * <p>You can use getOrderId() to get the value of orderId</p>
     *
     * @param orderId orderId
     */
    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    /**
     * Gets the value of price.
     *
     * @return the value of price.
     */
    public double getPrice() {
        return price;
    }

    /**
     * Sets the price.
     * <p>
     * <p>You can use getPrice() to get the value of price</p>
     *
     * @param price price
     */
    public void setPrice(double price) {
        this.price = price;
    }
}
