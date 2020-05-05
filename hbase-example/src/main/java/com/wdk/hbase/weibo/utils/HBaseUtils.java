package com.wdk.hbase.weibo.utils;

import com.wdk.hbase.weibo.constant.WeiBoConstant;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * @Description: Hbase工具类
 * 1:创建表命名空间
 * 2:创建表
 * 3:校验表是否存在
 * @Author:wang_dk
 * @Date:2020-05-05 8:58
 * @Version: v1.0
 **/

public class HBaseUtils {

    private static Connection connection;

    /**
     * @Description:
     * 私有化构造函数
     * @Date 2020-05-05 10:06
     * @Param
     * @return
     **/
    private HBaseUtils(){

    }

    public static Connection getConnection(){
        if(null == connection){
            synchronized (HBaseUtils.class){
                if(null == connection){
                    try {
                        connection = ConnectionFactory.createConnection(WeiBoConstant.CONF);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return connection;
    }



    /**
     * @return
     * @Description: 创建命名空间
     * @Date 2020-05-05 9:00
     * @Param
     **/
    public static void createNameSpace(String nameSpace) throws IOException {

        //获取admin对象
        Admin admin = getConnection().getAdmin();

        //创建 命名空间描述符
        NamespaceDescriptor nameSpaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        //创建命名空间
        admin.createNamespace(nameSpaceDescriptor);

        //关闭资源
        admin.close();
    }

    /**
     * @return
     * @Description: 判断表是否存在
     * @Date 2020-05-05 9:07
     * @Param
     **/
    private static boolean isTableExist(String tableName) throws IOException {

        //获取admin对象
        Admin admin = getConnection().getAdmin();

        boolean isExist = admin.tableExists(TableName.valueOf(tableName));

        //关闭资源
        admin.close();

        return isExist;
    }

    /**
     * @return
     * @Description: 创建表
     * @Date 2020-05-05 9:08
     * @Param tableName:表名称
     * versions:存储的最大版本数
     * cfs:列族
     **/
    public static void createTable(String tableName, int versions, String... cfs) throws IOException {
        //创建表必须有列族 先判断是否传入
        if (null == cfs || cfs.length == 0) {
            throw new InterruptedIOException("列族不能为空!");
        }

        //判断表是否已经存在
        if(isTableExist(tableName)){
            throw new InterruptedIOException("表名:"+tableName+"已存在!");
        }

        //获取admin对象
        Admin admin = getConnection().getAdmin();

        //表描述符
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //添加列族
        for (String cf : cfs) {
            //列族描述符
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);

            //添加版本
            columnDescriptor.setMinVersions(versions);

            tableDescriptor.addFamily(columnDescriptor);
        }

        admin.createTable(tableDescriptor);

        //关闭资源
        admin.close();
    }

    public static void main(String[] args) throws IOException {
        createNameSpace("wdk_hbase_weibo");
        createTable("wdk_hbase_weibo:content",3,"info");
    }


}
