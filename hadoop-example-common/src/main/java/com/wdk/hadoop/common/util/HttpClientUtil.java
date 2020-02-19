package com.wdk.hadoop.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

/**
 * @Description
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/2/19 14:31
 * @Since version 1.0.0
 */
public class HttpClientUtil {
    public static void main(String[] args) throws IOException {


        OkHttpClient okHttpClient = new OkHttpClient();

        String url = "https://www.qqca78.com/shipin/list-%E5%9B%BD%E4%BA%A7%E7%B2%BE%E5%93%81.html";
        Request request = new Request.Builder().url(url).build();
        Call call = okHttpClient.newCall(request);

        Response response = call.execute();
        String body = response.body().string();
        JSONObject bodyJson = JSON.parseObject(body);

        System.out.println(bodyJson.toJSONString());


//        for (int i=2000;i<10000;i++){
//            String url = "http://www.ftryvc.cn/index.php/App/Res/getResInfo?res_id="+(2091000+i)+"&uid=917&openid=";
//
//            Request request = new Request.Builder().url(url).build();
//            Call call = okHttpClient.newCall(request);
//            try {
//                Response response = call.execute();
//
//                String body = response.body().string();
//
//                JSONObject bodyJson = JSON.parseObject(body);
//                if(bodyJson.getInteger("ret") == 0){
//                    continue;
//                }
//
//                JSONObject packageJson = JSON.parseObject(bodyJson.get("package").toString());
//
//                String videoUrl = null != packageJson.get("videourl") ? packageJson.get("videourl").toString():"";
//
//                if(videoUrl.endsWith("m3u8")){
//                    System.out.println((2091000+i) +"~~~~~~~~~~~  "+videoUrl);
//                }
////                Thread.sleep(1000);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }
//    }
}
