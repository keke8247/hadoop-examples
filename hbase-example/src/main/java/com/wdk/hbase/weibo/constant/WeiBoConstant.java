package com.wdk.hbase.weibo.constant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @Description:
 * 常量类
 * @Author:wang_dk
 * @Date:2020-05-05 9:01
 * @Version: v1.0
 **/

public class WeiBoConstant {
    //连接配置
    public static final Configuration CONF = HBaseConfiguration.create();

    //命名空间
    public static final String NAME_SPACE = "weibo";

    //内容表
    public static final String CONTENT_TABLE = "weibo:content";
    public static final String CONTENT_TABLE_CF = "info";
    public static final int CONTENT_TABLE_VERSIONS = 1;

    //关系表
    public static final String RELATION_TABLE = "weibo:relation";
    public static final String RELATION_TABLE_CF1 = "attents";
    public static final String RELATION_TABLE_CF2 = "fans";
    public static final int RELATION_TABLE_VERSIONS = 1;

    //初始化页面 粉丝页面推送表
    public static final String INBOX_TABLE = "weibo:inbox";
    public static final String INBOX_TABLE_CF = "info";
    public static final int INBOX_TABLE_VERSIONS = 2;

    public static final Long MAX_TS = 9999999999999L;
}
