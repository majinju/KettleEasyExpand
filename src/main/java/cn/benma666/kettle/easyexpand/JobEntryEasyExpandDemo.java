/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettle.easyexpand;

import cn.benma666.kettle.jobentry.easyexpand.JobEntryEasyExpandRunBase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * JobEntryEasyExpand 运行示例<br/>
 * date: 2016年6月29日 <br/>
 * @author jingma
 * @version 
 */
public class JobEntryEasyExpandDemo extends JobEntryEasyExpandRunBase{

    /**
    * 
    * @see cn.benma666.kettle.jobentry.easyexpand.JobEntryEasyExpandRunBase#run()
    */
    @Override
    protected boolean run() throws Exception {
        //这里写自己的业务
        jeku.logBasic(jeku.getConfigInfo());
        jeku.logBasic(configInfo.toJSONString());
        return true;
    }

    /**
     * 
     * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#getDefaultConfigInfo(org.pentaho.di.trans.TransMeta, java.lang.String)
     */
     @Override
     public String getDefaultConfigInfo() throws Exception{
        //创建一个JSON对象，用于构建配置对象，避免直接拼字符串构建JSON字符串
        JSONObject params = new JSONObject();
        //设置一个参数key1
        params.put("key1", "");
        //创建一个JSON数组对象，用于存放数组参数
        JSONArray arr = new JSONArray();
        arr.add("arr1");
        arr.add("arr2");
        params.put("array", arr);
        //生成的参数样例
        //{
        //  "array":[
        //          "arr1",
        //          "arr2"
        //  ],
        //  "key1":""
        //}
        //返回格式化后的默认JSON配置参数，供使用者方便快捷的修改配置
        return JSON.toJSONString(params, true);
//         return "select *\n from dual";
    }
}
