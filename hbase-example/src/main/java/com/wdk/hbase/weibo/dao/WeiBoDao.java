package com.wdk.hbase.weibo.dao;

import com.wdk.hbase.weibo.constant.WeiBoConstant;
import com.wdk.hbase.weibo.utils.HBaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Array;
import java.util.ArrayList;

/**
 * @Description:
 * 微博相关操作
 * @Author:wang_dk
 * @Date:2020-05-05 9:57
 * @Version: v1.0
 **/

public class WeiBoDao {

    /**
     * @Description:
     * 发布微博
     * @Date 2020-05-05 15:22
     * @Param
     * @return
     **/
    public static void publishWeiBo(String uid,String content) throws IOException {
        //*******************step1 插入数据到微博内容表*******************
        //获取content表对象
        Table contTable = HBaseUtils.getConnection().getTable(TableName.valueOf(WeiBoConstant.CONTENT_TABLE));

        //微博的rowKey
        /*
        * rowKey设计思路. 直接: uid_时间戳 这样最先发布的微博 在scan的时候 最先被查出来.
        *   显示业务中 往往最后发布的微博 被查的概率较高. 所以把 时间戳反转一下  9999999999999-时间戳
        *   最先发布的时间戳最大,这样最先发布的最后被查询出来 取最近topN 就比较方便了
        * */
        String rowKey = uid+"_"+(WeiBoConstant.MAX_TS-System.currentTimeMillis());

        //定义put对象
        Put contentPut = new Put(Bytes.toBytes(rowKey));
        //构建put对象的内容
        contentPut.addColumn(Bytes.toBytes(WeiBoConstant.CONTENT_TABLE_CF),Bytes.toBytes("content"),Bytes.toBytes(content));

        //插数据
        contTable.put(contentPut);

        //*******************step2 更新关注该博主的粉丝页面表***************
        //获取关系表对象
        Table relationTable = HBaseUtils.getConnection().getTable(TableName.valueOf(WeiBoConstant.RELATION_TABLE));

        //构建get 提交 查询出fans列族
        Get fansGet = new Get(Bytes.toBytes(uid));
        fansGet.addFamily(Bytes.toBytes(WeiBoConstant.RELATION_TABLE_CF2));
        //获取该博主的fans
        Result fansResult = relationTable.get(fansGet);

        //构建一个Put集合 批量提交
        ArrayList<Put> fansPuts = new ArrayList<>();

        //取出fans 构建fans页面表的put对象
        for (Cell cell : fansResult.rawCells()) {
            Put fansPut = new Put(CellUtil.cloneQualifier(cell));

            fansPut.addColumn(Bytes.toBytes(WeiBoConstant.INBOX_TABLE_CF),Bytes.toBytes(uid),Bytes.toBytes(rowKey));

            fansPuts.add(fansPut);
        }

        if(fansPuts.size()>0){
            //获取 fans推送表 对象
            Table inBoxTable = HBaseUtils.getConnection().getTable(TableName.valueOf(WeiBoConstant.INBOX_TABLE));

            inBoxTable.put(fansPuts);

            //关闭资源
            inBoxTable.close();
        }

        //*************************step3 收尾*************************
        relationTable.close();
        contTable.close();
    }

    /**
     * @Description:
     * 关注 A--->B,C,D A关注BCD
     * @Date 2020-05-05 11:14
     * @Param
     *          uid:当前操作人
     *          attents:被关注用户
     * @return
     **/
    public static void addAttents(String uid,String... attents) throws IOException {
        if(null == attents || attents.length == 0){
            throw new InterruptedIOException("请添加关注人!!!");
        }

        //***********************step1 维护关注关系表************************
        //当前操作用户的 attents列族 增加关注人
        //被关注的 fans列族 增加一个粉丝
        //获取表对象
        Table relationTable = HBaseUtils.getConnection().getTable(TableName.valueOf(WeiBoConstant.RELATION_TABLE));

        //构建Put集合
        ArrayList<Put> relPuts = new ArrayList<>();

        //构建当前操作人的Put对象
        Put uidPut = new Put(Bytes.toBytes(uid));

        //构建被关注人的Fans Put对象
        for (String attent : attents) {
            //uidPut 赋值
            uidPut.addColumn(Bytes.toBytes(WeiBoConstant.RELATION_TABLE_CF1),Bytes.toBytes(attent),Bytes.toBytes(attent));

            Put fansPut = new Put(Bytes.toBytes(attent));
            //fansPut 赋值
            fansPut.addColumn(Bytes.toBytes(WeiBoConstant.RELATION_TABLE_CF2),Bytes.toBytes(uid),Bytes.toBytes(uid));
            relPuts.add(fansPut);
        }

        relPuts.add(uidPut);
        relationTable.put(relPuts);

        //***********************step2 把关注人最近发布的微博 加载到当前操作人 INBOX
        //获取content表对象
        Table contTable = HBaseUtils.getConnection().getTable(TableName.valueOf(WeiBoConstant.CONTENT_TABLE));

        //构建Put对象
        Put inBoxPut = new Put(Bytes.toBytes(uid));

        //Scan 关注用户的微博数据
        for (String attent : attents) {
            //构建Scan对象 在比较大小的时候 | 比 _ 大 所以 startRow= attent_,stopRow=attent| 把attent用户发的微博全部查出
            Scan scan = new Scan(Bytes.toBytes(attent+"_"),Bytes.toBytes(attent+"|"));
            ResultScanner contTableScanner = contTable.getScanner(scan);

            //由于前面rowKey设计的时候 最近发布的微博rowKey最小 可以最先查出 所以取Top3就是最近3条微博 写入InBox
            Result[] next = contTableScanner.next(3);
            for (Result result : next) {
                inBoxPut.addColumn(Bytes.toBytes(WeiBoConstant.INBOX_TABLE_CF),Bytes.toBytes(attent),result.getRow());
            }
        }

        //校验 关注用户是否发过微博
        if(!inBoxPut.isEmpty()){
            //获取INBOX表对象
            Table inboxTable = HBaseUtils.getConnection().getTable(TableName.valueOf(WeiBoConstant.INBOX_TABLE));
            inboxTable.put(inBoxPut);

            inboxTable.close();
        }

        //************************step3 收尾*******************************
        contTable.close();
        relationTable.close();
    }

    /**
     * @Description:
     * 取消关注
     * @Date 2020-05-05 14:45
     * @Param
     *          uid:操作用户
     *          dels:被取关用户
     * @return
     **/
    public static void deleteAttents(String uid,String... dels) throws IOException {
        /***************************step1 删除关系表 attents列的数据*****************************/
        //获取 用户关系 表对象
        Table relaTable = HBaseUtils.getConnection().getTable(TableName.valueOf(WeiBoConstant.RELATION_TABLE));

        //创建当前操作人的Delete对象  删除attents
        Delete uidDel = new Delete(Bytes.toBytes(uid));

        //构建一个集合 批量删除
        ArrayList<Delete> deletes = new ArrayList<>();

        for (String del : dels) {
            //负责 删除被取关用户的 Qualifier
            uidDel.addColumns(Bytes.toBytes(WeiBoConstant.RELATION_TABLE_CF1),Bytes.toBytes(del));

            //创建被取关人的Delete对象 把fans 为uid 的Qualifier删掉
            Delete delDel = new Delete(Bytes.toBytes(del));
            delDel.addColumns(Bytes.toBytes(WeiBoConstant.RELATION_TABLE_CF2),Bytes.toBytes(uid));

            deletes.add(delDel);
        }

        deletes.add(uidDel);

        //执行批量删除
        relaTable.delete(deletes);

        /***************************step2 删除INBOX表 被取关用户的数据*****************************/
        //获取INBOX表对象
        Table inboxTable = HBaseUtils.getConnection().getTable(TableName.valueOf(WeiBoConstant.INBOX_TABLE));

        //构建Delete对象
        Delete uidInboxDel = new Delete(Bytes.toBytes(uid));
        for (String del : dels) {
            uidInboxDel.addColumns(Bytes.toBytes(WeiBoConstant.INBOX_TABLE_CF),Bytes.toBytes(del));
        }

        //执行删除操作
        inboxTable.delete(uidInboxDel);


        /***************************step3 收尾*****************************************/
        inboxTable.close();
        relaTable.close();
    }

    /**
     * @Description:
     *      获取当前操作用户的初始化页面
     * @Date 2020-05-05 15:23
     * @Param
     * @return
     **/
    public static void homePage(String uid) throws IOException {
        //获取INBOX表对象
        Table inboxTable = HBaseUtils.getConnection().getTable(TableName.valueOf(WeiBoConstant.INBOX_TABLE));

        //构建Get对象 获取关注用户的微博列表
        Get inboxGet = new Get(Bytes.toBytes(uid));
        //设置最大版本 把所有版本的数据都取出来
        inboxGet.setMaxVersions();

        //当前操作人 关注的所用用户的微博top3
        Result result = inboxTable.get(inboxGet);

        if(!result.isEmpty()){
            //获取微博内容 表对象
            Table contTable = HBaseUtils.getConnection().getTable(TableName.valueOf(WeiBoConstant.CONTENT_TABLE));

            for (Cell cell : result.rawCells()) {
                //构建获取微博内容的Get对象
                Get contGet = new Get(CellUtil.cloneValue(cell));

                //获取微博内容
                Result contentResult = contTable.get(contGet);

                //循环遍历 Cell
                for (Cell contentCell : contentResult.rawCells()) {
                    //拼接打印微博内容
                    System.out.println("RK:"+Bytes.toString(CellUtil.cloneRow(contentCell))+"|CF:"+Bytes.toString(CellUtil.cloneFamily(contentCell))+
                            "|CQ:"+Bytes.toString(CellUtil.cloneQualifier(contentCell))+"|value:"+Bytes.toString(CellUtil.cloneValue(contentCell)));
                }
            }
            contTable.close();
        }

        //收尾关闭资源
        inboxTable.close();
    }

    /**
     * @Description:
     * 获取某个用户的所有微博内容
     * @Date 2020-05-05 15:34
     * @Param
     * @return
     **/
    public static void getWeiboByUid(String uid) throws IOException {
        //获取微博内容表对象
        Table contTable = HBaseUtils.getConnection().getTable(TableName.valueOf(WeiBoConstant.CONTENT_TABLE));

        //构建Scan对象
        Scan scan = new Scan();

        //使用Filter的方式查询数据. 也可以使用 startRow,stopRow查询.
        Filter rkFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(uid+"_")); //如果RK中包含 uid_ 则返回
        scan.setFilter(rkFilter);

        ResultScanner contTableScanner = contTable.getScanner(scan);

        //获取微博数据并循环打印
        for (Result contResult : contTableScanner) {

            for (Cell contentCell : contResult.rawCells()) {
                //拼接打印微博内容
                System.out.println("RK:"+Bytes.toString(CellUtil.cloneRow(contentCell))+"|CF:"+Bytes.toString(CellUtil.cloneFamily(contentCell))+
                        "|CQ:"+Bytes.toString(CellUtil.cloneQualifier(contentCell))+"|value:"+Bytes.toString(CellUtil.cloneValue(contentCell)));
            }
        }

        //收尾
        contTable.close();
    }

}
