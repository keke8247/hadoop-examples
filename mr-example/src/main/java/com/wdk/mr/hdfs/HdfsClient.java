package com.wdk.mr.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @Description
 * @Author rdkj
 * @CreatTime 2020/6/2 15:20
 * @Since version 1.0.0
 */
public class HdfsClient {

    @Test
    public void testMakeDirs() throws Exception {
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"),configuration,"root");

        fs.mkdirs(new Path("/testPath/wdk"));

        fs.copyFromLocalFile(new Path("e:/test"),new Path(""));

        fs.close();
    }

}
