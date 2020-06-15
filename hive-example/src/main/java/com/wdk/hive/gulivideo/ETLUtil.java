package com.wdk.hive.gulivideo;

/**
 * @Description:
 * @Author:wang_dk
 * @Date:2020-06-06 9:15
 * @Version: v1.0
 **/

public class ETLUtil {
    public static String oriString2ETLString(String ori) {
        StringBuilder etlString = new StringBuilder();
        String[] splits = ori.split("\t");
        if (splits.length < 9) return null;
        splits[3] = splits[3].replace(" ", "");
        for (int i = 0; i < splits.length; i++) {
            if (i < 9) {
                if (i == splits.length - 1) {
                    etlString.append(splits[i]);
                } else {
                    etlString.append(splits[i] + "\t");
                }
            } else {
                if (i == splits.length - 1) {
                    etlString.append(splits[i]);
                } else {
                    etlString.append(splits[i] + "&");
                }
            }
        }
        return etlString.toString();
    }
}
