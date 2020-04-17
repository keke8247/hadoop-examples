package com.wdk.mr.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * @Description:
 *  测试Hadoop 自带的压缩与解压缩
 * @Author:wang_dk
 * @Date:2020/4/14 0014 21:41
 * @Version: v1.0
 **/

public class TestCompress {

    public static void main(String[] args) throws Exception{
        //Hadoop默认压缩 org.apache.hadoop.io.compress.DefaultCodec
        //gzip压缩 org.apache.hadoop.io.compress.GzipCodec
        //bzip2压缩 org.apache.hadoop.io.compress.BZip2Codec

        compress("D:\\input\\compress\\test.docx","org.apache.hadoop.io.compress.BZip2Codec");

        //解压缩
//        decompress("D:\\\\input\\\\compress\\test.docx.bz2");
    }

    /**
     * @Description:
     * 解压缩
     * @Date 2020/4/14 0014 21:58
     * @Param filePath 要解压的文件
     * @return
     **/
    private static void decompress(String filePath) throws IOException {
        //1.判断文件是否可以解压 (Hadoop是否支持此压缩格式)
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = codecFactory.getCodec(new Path(filePath));
        if(null == codec){  //没有对应的压缩 算法 GG
            System.out.println("不支持的文件类型.GG");
            return;
        }

        //2.获取输入流
        FileInputStream fis = new FileInputStream(new File(filePath));
        //2.1 带有解压功能的输入流
        CompressionInputStream cis = codec.createInputStream(fis);

        //3.获取输出流
        FileOutputStream fos = new FileOutputStream(new File(filePath + ".decode"));

        //4.流的对拷
        IOUtils.copyBytes(cis,fos,1024*1024,false);

        //5.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);

    }

    /**
     * @Description:
     * 压缩
     * @Date 2020/4/14 0014 21:44
     * @Param filePath 要压缩的文件
     *          method 采用的压缩算法
     * @return
     **/
    private static void compress(String filePath, String method) throws IOException, ClassNotFoundException {
        //1.获取输入流
        FileInputStream fis = new FileInputStream(new File(filePath));

        //2.获取输出流
        //2.1 获取压缩方法压缩出来的文件格式
        Class compressClass = Class.forName(method);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(compressClass,new Configuration());

        FileOutputStream fos = new FileOutputStream(filePath+codec.getDefaultExtension());
        CompressionOutputStream cos = codec.createOutputStream(fos);    //带压缩功能的输出流

        //3.流的对拷
        IOUtils.copyBytes(fis,cos,1024*1024,false);

        //4.关闭资源
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }


}
