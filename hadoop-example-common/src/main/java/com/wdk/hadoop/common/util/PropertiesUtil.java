package com.wdk.hadoop.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @Description:
 * 配置文件读取工具类
 * @Author:wang_dk
 * @Date:2020/2/9 0009 12:01
 * @Version: v1.0
 **/

public class PropertiesUtil {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

    private static Properties properties;

    static {
        properties = new Properties();
    }

    private static void loadProperties(String propertiesFilePath){
        try {
            properties.load(PropertiesUtil.class.getClassLoader().getResourceAsStream(propertiesFilePath));
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("读取配置文件失败，请确认文件：{}是否存在。",propertiesFilePath);
        }
    }


    public static String getValue(String propertyName,String propertyFilePath){
        String value = properties.getProperty(propertyName);

        if(null == value){
            loadProperties(propertyFilePath);
            value = properties.getProperty(propertyName,null);
        }

        return value;
    }
}
