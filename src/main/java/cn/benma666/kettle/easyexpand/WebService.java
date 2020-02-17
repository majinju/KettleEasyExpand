/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettle.easyexpand;

import java.util.Map;
import java.util.Vector;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import cn.benma666.myutils.WebserviceUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * WebService插件<br/>
 * date: 2016年6月29日 <br/>
 * @author jingma
 * @version 
 */
public class WebService extends EasyExpandRunBase{
    private static final String WSDL_RUL = "WSDL地址";
    private static final String SERVICE_NAME = "服务名称";
    private static final String PORT_NAME = "端口名称";
    private static final String OPERATION_NAME = "操作名称";
    private static final String PARAMS_ARR = "参数字段列表";
    private static final String TIME_OUT = "超时时长";
    /**
    * ws操作工具类
    */
    private WebserviceUtil invoker;
    /**
    * 具体处理每一行数据
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
    */
    @Override
    protected void disposeRow(Object[] outputRow) {
        Vector<String> ps = new Vector<String>();
        for(String p:configInfo.getJSONArray(PARAMS_ARR).toArray(new String[]{})){
            ps.addElement(outputRow[getFieldIndex(p)].toString());
        }
        try {
            Map<?, ?> result = invoker.invoke(configInfo.getString(SERVICE_NAME), 
                    configInfo.getString(PORT_NAME),
                    configInfo.getString(OPERATION_NAME), ps,
                    configInfo.getIntValue(TIME_OUT)*1000);
            outputRow[getFieldIndex("RESULT")] = JSON.toJSON(result);
        } catch (Exception e) {
            throw new RuntimeException("请求失败",e);
        }
    }
    /**
    * 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#init()
    */
    @Override
    protected void init() {
        String url = configInfo.getString(WSDL_RUL);
        try {
            invoker = new WebserviceUtil(url);
        } catch (Exception e) {
            throw new RuntimeException("url:"+url,e);
        }
    }

    /**
     * 
     * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#getDefaultConfigInfo(org.pentaho.di.trans.TransMeta, java.lang.String)
     */
     @Override
     public String getDefaultConfigInfo(TransMeta transMeta, String stepName) throws Exception{
        JSONObject params = new JSONObject();
        params.put(WSDL_RUL, "");
        params.put(SERVICE_NAME, "");
        params.put(PORT_NAME, "");
        params.put(OPERATION_NAME, "");
        params.put(TIME_OUT, "15");
        //创建一个JSON数组对象，用于存放数组参数
        JSONArray arr = new JSONArray();
        arr.add("arr1");
        arr.add("arr2");
        params.put(PARAMS_ARR, arr);
        return JSON.toJSONString(params, true);
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        //添加输出到下一步的字段
        addField(r,"RESULT",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"请求结果");
    }
}
