package com.wdk.hive.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 *  自定义UDTF函数  继承 GenericUDTF 实现 initialize() close() process() 方法
 * @Author wangdk, wangdk@erongdu.com
 * @CreatTime 2020/3/24 16:34
 * @Since version 1.0.0
 */
public class EventJsonUDTF extends GenericUDTF{

    //该方法中，我们将指定输出参数的名称和参数类型：
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs)
            throws UDFArgumentException {
        List<String> structFieldNames = new ArrayList<>();
        List<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();

        structFieldNames.add("event_name");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        structFieldNames.add("event_value");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames,structFieldObjectInspectors);
    }

    //输入1条记录，输出若干条结果
    @Override
    public void process(Object[] args) throws HiveException {
        String input = args[0].toString();

        if(StringUtils.isBlank(input)){
            return;
        }
        //获取输入数据
        try {
            JSONArray events = new JSONArray(input);

            if(events.length()==0){
                return;
            }

            String[] results = new String[2];

            //循环解析数据
            for(int i=0;i<events.length();i++){
                String event_name = events.getJSONObject(i).getString("en");
                results[0] = event_name;
                results[1] = events.getString(i);

                forward(results);
            }

        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
