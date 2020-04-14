package com.wdk.mr.reduce_join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020/4/13 0013 20:41
 * @Version: v1.0
 **/

public class TableBean implements Writable {

    private String id;
    private String pid;
    private int amount;
    private String pName;
    private String flag;

    public TableBean() {
    }

    public TableBean(String id, String pid, int amount, String pName, String flag) {
        this.id = id;
        this.pid = pid;
        this.amount = amount;
        this.pName = pName;
        this.flag = flag;
    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pName);
        dataOutput.writeUTF(flag);
    }

    //反序列化  顺序和序列化顺序一致
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pName = dataInput.readUTF();
        this.flag = dataInput.readUTF();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() { //重写toString方法  定义输出数据的格式
        return id +"\t" + amount +"\t" + pName;
    }
}
