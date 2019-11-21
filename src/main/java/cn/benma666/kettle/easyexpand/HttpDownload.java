/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettle.easyexpand;

import java.net.URLEncoder;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import cn.benma666.exception.MyException;
import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import cn.benma666.myutils.StringUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * Http下载文件<br/>
 * date: 2016年6月29日 <br/>
 * @author jingma
 * @version 
 */
public class HttpDownload extends EasyExpandRunBase{
    private static final String POST_URL = "请求url";
    private static final String POST_PARAM = "请求参数(请求参数字段:源流数据参数字段)";
    private static final String POST_CFQQCS = "请求报错，重新请求的次数";
    private static final String POST_CSSC = "请求超时时间";
    private static final String POST_FHJGMC = "返回结果名称";
    private static final String POST_QQBM = "请求编码";
    private static final String POST_WJM = "文件名";
    private static final String POST_BCLJ = "保存路径";
    private static final String POST_HLYC = "忽略异常";

    /**
    * 具体处理每一行数据
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
    */
    @Override
    protected void disposeRow(Object[] outputRow) throws Exception {
        String url = configInfo.getString(POST_URL);
        if(url.startsWith("lyzd:")){
            url = outputRow[getFieldIndex(url.substring("lyzd:".length()))].toString();
        }
        int cfqqcs = configInfo.getIntValue(POST_CFQQCS);
        int cssc = configInfo.getIntValue(POST_CSSC);
        String qqbm = configInfo.getString(POST_QQBM);
        String wjm = configInfo.getString(POST_WJM);
        if(wjm.startsWith("lyzd:")){
            wjm = outputRow[getFieldIndex(wjm.substring("lyzd:".length()))].toString();
        }
        String bclj = configInfo.getString(POST_BCLJ);
        if(bclj.startsWith("lyzd:")){
            bclj = outputRow[getFieldIndex(bclj.substring("lyzd:".length()))].toString();
        }
        //请求参数
        String params = "";
        //拼接post请求参数
        JSONObject paramObj = configInfo.getJSONObject(POST_PARAM);
        for(String key:paramObj.keySet()){
            try {
                if(outputRow[getFieldIndex(paramObj.getString(key))]!=null){
                    String value = outputRow[getFieldIndex(paramObj.getString(key))].toString();
                    if(value.length() != value.getBytes().length){
                        //将含有中文的字符串转为UTF-8编码格式
                        value = URLEncoder.encode(value, qqbm);
                    }
                    params = params + key + "=" + value + "&";
                }
            } catch (Exception e) {
                ku.logError("配置的参数：" + paramObj.getString(key) + "在源流数据中不存在！",e);
            }
        }
        //去掉最后一位 '&'字符
        if(StringUtil.isNotBlank(params)){
            params = params.substring(0, params.length()-1);
        }
        String msg = ""; 
        try {
            msg = cn.benma666.myutils.HttpUtil.downLoadFromUrl(url, wjm, bclj, cssc).getAbsolutePath();
        } catch (Exception e) {
            ku.logError("第一次请求报错！Url:" + url + ";Params:" + params, e);
            for(int i=1;i<=cfqqcs;i++){
                boolean flag = true;
                try {
                    msg = cn.benma666.myutils.HttpUtil.downLoadFromUrl(url, wjm, bclj, cssc).getAbsolutePath();
                } catch (Exception e2) {
                    flag = false;
                    if(i!=cfqqcs){
                        ku.logBasic("重新第"+i+"次请求报错！Url:" + url + ";Params:" + params, e2);
                    }else{
                        if(configInfo.getBooleanValue(POST_HLYC)){
                            ku.logError("重新第"+i+"次请求报错！Url:" + url + ";Params:" + params, e2);
                        }else{
                            throw new MyException("重新第"+i+"次请求报错！Url:" + url + ";Params:" + params,e2);
                        }
                    }
                }
                if(flag){
                    ku.logBasic("重新第"+i+"次请求成功！Url:" + url + ";Params:" + params);
                    break;
                }
            }
        }
        outputRow[getFieldIndex(configInfo.getString(POST_FHJGMC))] = msg;
        ku.putRow(data.outputRowMeta, outputRow);
    }
    /**
    * 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#init()
    */
    @Override
    protected void init() {
    }
    /**
    * 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#end()
    */
    @Override
    protected void end() {
    }

    /**
     * 
     * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#getDefaultConfigInfo(org.pentaho.di.trans.TransMeta, java.lang.String)
     */
     @Override
     public String getDefaultConfigInfo(TransMeta transMeta, String stepName) throws Exception{
        //创建一个JSON对象，用于构建配置对象，避免直接拼字符串构建JSON字符串
        JSONObject params = new JSONObject();
        //设置一个参数key1
        params.put(POST_URL, "");
        params.put(POST_CSSC, 60000);
        params.put(POST_QQBM, "UTF-8");
        params.put(POST_CFQQCS, 5);
        params.put(POST_FHJGMC, "result");
        params.put(POST_WJM, "");
        params.put(POST_BCLJ, "/tmp");
        params.put(POST_HLYC, true);
        params.put(POST_PARAM, new JSONObject());
        return JSON.toJSONString(params, true);
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        //添加输出到下一步的字段
        addField(r,configInfo.getString(POST_FHJGMC),ValueMeta.TYPE_STRING,
                ValueMeta.TRIM_TYPE_BOTH,origin,"请求返回的结果");
    }
}
