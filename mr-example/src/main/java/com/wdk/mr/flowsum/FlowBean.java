package com.wdk.mr.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/4/2 10:28
 * @Since version 1.0.0
 */
public class FlowBean implements Writable{
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean() {
        super();
    }

    //toString
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow ;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
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
