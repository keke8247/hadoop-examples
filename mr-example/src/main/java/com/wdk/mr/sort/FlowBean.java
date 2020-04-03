package com.wdk.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/3 15:46
 * @Since version 1.0.0
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean() {
        super();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {

        int result = 0;
        if(sumFlow > o.sumFlow){
            result = -1;
        }else if(sumFlow < o.sumFlow){
            result = 1;
        }
        return result;
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    /**
     * Gets the value of upFlow.
     *
     * @return the value of upFlow.
     */
    public long getUpFlow() {
        return upFlow;
    }

    /**
     * Sets the upFlow.
     * <p>
     * <p>You can use getUpFlow() to get the value of upFlow</p>
     *
     * @param upFlow upFlow
     */
    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    /**
     * Gets the value of downFlow.
     *
     * @return the value of downFlow.
     */
    public long getDownFlow() {
        return downFlow;
    }

    /**
     * Sets the downFlow.
     * <p>
     * <p>You can use getDownFlow() to get the value of downFlow</p>
     *
     * @param downFlow downFlow
     */
    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    /**
     * Gets the value of sumFlow.
     *
     * @return the value of sumFlow.
     */
    public long getSumFlow() {
        return sumFlow;
    }

    /**
     * Sets the sumFlow.
     * <p>
     * <p>You can use getSumFlow() to get the value of sumFlow</p>
     *
     * @param sumFlow sumFlow
     */
    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
}
