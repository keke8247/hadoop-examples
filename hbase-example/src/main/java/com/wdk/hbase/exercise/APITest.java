package com.wdk.hbase.exercise;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020-05-03 16:27
 * @Version: v1.0
 **/

public class APITest {

    private static Configuration configuration;

    private static HBaseAdmin hBaseAdmin;

    private static HTable hTable;

    @Before
    public void initConf() throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        //创建HbaseAdmin对象 操作表
        hBaseAdmin = (HBaseAdmin) ConnectionFactory.createConnection(configuration).getAdmin();

        hTable = (HTable) ConnectionFactory.createConnection(configuration).getTable(TableName.valueOf("wdk_hbase:student"));
    }

    @Test
    public void isTableExist() throws IOException {

        boolean isExist = hBaseAdmin.tableExists("wdk_hbase:student");

        System.out.println(isExist);

        hBaseAdmin.close();
    }

    @Test
    public void createTable() throws IOException {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("wdk_hbase:student"));
        hTableDescriptor.addFamily(new HColumnDescriptor("info"));
        hTableDescriptor.addFamily(new HColumnDescriptor("msg"));
        hTableDescriptor.addFamily(new HColumnDescriptor("likes"));

        if (hBaseAdmin.tableExists(hTableDescriptor.getTableName())) { //判断表是否存在
            System.out.println("表已存在.请先删除再创建.");
            return;
        }
        hBaseAdmin.createTable(hTableDescriptor);

        hBaseAdmin.close();
    }

    //添加数据
    @Test
    public void addRowData() throws IOException {
        for(int i=0;i<10;i++){
            Put put = new Put(Bytes.toBytes("1001"+i));   //构建put对象,需要传入一个rowKey
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"+i));
            put.addColumn(Bytes.toBytes("msg"), Bytes.toBytes("qq"), Bytes.toBytes("82474819"+i));
            put.addColumn(Bytes.toBytes("likes"), Bytes.toBytes("ball"), Bytes.toBytes("basktball"+i));

            hTable.put(put);
        }

        hTable.close();

        System.out.println("数据插入成功!");

    }

    //获取全表数据
    @Test
    public void scanTable() throws IOException {


        ResultScanner tableScanner = hTable.getScanner(new Scan());

        Iterator<Result> iterator = tableScanner.iterator();
        while (iterator.hasNext()){
            Result result = iterator.next();
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //获取rowKey
                System.out.println("rowKey:"+Bytes.toString(CellUtil.cloneRow(cell)));
                //columnFamily
                System.out.println("columnFamily:"+Bytes.toString(CellUtil.cloneFamily(cell)));
                //columnQualifier
                System.out.println("columnQualifier:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                //value
                System.out.println("value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        hTable.close();
    }

    //获取某一行数据 根据rowKey
    @Test
    public void getOneRow() throws IOException {
        Get get = new Get(Bytes.toBytes("10011"));
        Result result = hTable.get(get);

        for (Cell cell : result.listCells()) { //listCells() 其实底层调用的就是 rowCells()
            //获取rowKey
            System.out.println("rowKey:"+Bytes.toString(CellUtil.cloneRow(cell)));
            //columnFamily
            System.out.println("columnFamily:"+Bytes.toString(CellUtil.cloneFamily(cell)));
            //columnQualifier
            System.out.println("columnQualifier:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
            //value
            System.out.println("value:"+Bytes.toString(CellUtil.cloneValue(cell)));
        }
        hTable.close();
    }

    //获取某一行数据的 某一个columnQualifier
    @Test
    public void getOneRowQualifier() throws IOException {
        Get get = new Get(Bytes.toBytes("1001"));
        get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));

        Result result = hTable.get(get);

        for (Cell cell : result.listCells()) { //listCells() 其实底层调用的就是 rowCells()
            //获取rowKey
            System.out.println("rowKey:"+Bytes.toString(CellUtil.cloneRow(cell)));
            //columnFamily
            System.out.println("columnFamily:"+Bytes.toString(CellUtil.cloneFamily(cell)));
            //columnQualifier
            System.out.println("columnQualifier:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
            //value
            System.out.println("value:"+Bytes.toString(CellUtil.cloneValue(cell)));
        }
        hTable.close();
    }

    //删除数据
    @Test
    public void deleteRowData() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("10011")); //删除一条数据
        hTable.delete(delete);

        List<Delete> deletes = new ArrayList<>(); //删除多条数据
        deletes.add(delete);
        hTable.delete(deletes);

        hTable.close();
    }

    //删除表
    @Test
    public void dropTable() throws IOException {
        if(hBaseAdmin.tableExists("wdk_hbase:student")){
            hBaseAdmin.disableTable("wdk_hbase:student");
            hBaseAdmin.deleteTable("wdk_hbase:student");
        }
        hBaseAdmin.close();
    }


}
