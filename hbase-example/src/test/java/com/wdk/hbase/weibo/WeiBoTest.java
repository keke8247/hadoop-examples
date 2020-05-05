package com.wdk.hbase.weibo;

import com.wdk.hbase.weibo.constant.WeiBoConstant;
import com.wdk.hbase.weibo.dao.WeiBoDao;
import com.wdk.hbase.weibo.utils.HBaseUtils;
import org.junit.Test;

import java.io.IOException;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020-05-05 11:47
 * @Version: v1.0
 **/

public class WeiBoTest {
    //创建命名空间测试
    @Test
    public void createNameSpaceTest() throws IOException {
        HBaseUtils.createNameSpace("weibo");
    }

    //创建表测试
    @Test
    public void createTabelTest() throws IOException {
        HBaseUtils.createTable(WeiBoConstant.CONTENT_TABLE,WeiBoConstant.CONTENT_TABLE_VERSIONS,WeiBoConstant.CONTENT_TABLE_CF);
        HBaseUtils.createTable(WeiBoConstant.RELATION_TABLE,WeiBoConstant.RELATION_TABLE_VERSIONS,WeiBoConstant.RELATION_TABLE_CF1,WeiBoConstant.RELATION_TABLE_CF2);
        HBaseUtils.createTable(WeiBoConstant.INBOX_TABLE,WeiBoConstant.INBOX_TABLE_VERSIONS,WeiBoConstant.INBOX_TABLE_CF);
    }

    //发布微博
    @Test
    public void publishWeiboTest() throws IOException {
        WeiBoDao.publishWeiBo("zmg","这是zmg发布的最后1条微博!");
    }

    //添加关注
    @Test
    public void addAttentsTest() throws IOException {
//        WeiBoDao.addAttents("wd","janms","lisuhao","kobe","zmg");
        WeiBoDao.addAttents("kobe","janms","lisuhao","wd","zmg");
    }

    //取关
    @Test
    public void deleteAttentsTest() throws IOException {
        WeiBoDao.deleteAttents("kobe","wd","janms");
    }

    //初始化页面
    @Test
    public void homePageTest() throws IOException {
        WeiBoDao.homePage("wd");
    }

    //获取某个人的所有微博
    @Test
    public void getWeiBoByUidTest() throws IOException {
        WeiBoDao.getWeiboByUid("kobe");
    }
}
