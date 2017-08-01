/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettle.easyexpand;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import cn.benma666.kettle.common.Dict;
import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import cn.benma666.myutils.DateUtil;
import cn.benma666.myutils.SfzhUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 数据转换<br/>
 * date: 2016年6月29日 <br/>
 * @author jingma
 * @version 
 */
public class DataTransform extends EasyExpandRunBase{

    //参数
    private static final String TRANS_INFO = "转换信息";
    private static final String TRANS_FIELD = "验证字段";
    private static final String TRANS_RULE = "验证规则";
    private static final String RESULT_LATER = "结果后缀";
    private static final String TRANS_RULE_DATA = "验证规则数据";
    
    private static final String TRANS_RULE_DATA_EXCLUDES_FIELDS = "排除的字段";
    private static final String TRANS_RULE_DATA_FGF = "分隔符";
    private static final String TRANS_RULE_DATA_FBF = "封闭符";

    //规则
    private static final String RULE_DATE_FORMAT = "时间格式";
    private static final String RULE_SFZH = "身份证格式";
    private static final String RULE_DICT = "字典";
    private static final String RULE_TOJSON = "合并为JSON";
    private static final String RULE_TOTXT = "合并为TXT";
    
    private static Map<String,List<String>> efsMap = new HashMap<String,List<String>>();

    /**
    * 具体处理每一行数据
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
    */
    @Override
    protected void disposeRow(Object[] outputRow) {
        for(JSONObject ti:configInfo.getJSONArray(TRANS_INFO).toArray(new JSONObject[]{})){
            switch (ti.getString(TRANS_RULE)) {
            case RULE_DATE_FORMAT:
                transDateFormat(outputRow[getFieldIndex(ti.getString(TRANS_FIELD))], ti,outputRow);
                break;
            case RULE_SFZH:
                transSfzh(outputRow[getFieldIndex(ti.getString(TRANS_FIELD))], ti,outputRow);
                break;
            case RULE_DICT:
                transDict(outputRow[getFieldIndex(ti.getString(TRANS_FIELD))], ti,outputRow);
                break;
            case RULE_TOJSON:
                transToJson(outputRow[getFieldIndex(ti.getString(TRANS_FIELD))], ti,outputRow);
                break;
            case RULE_TOTXT:
                transToTxt(outputRow[getFieldIndex(ti.getString(TRANS_FIELD))], ti,outputRow);
                break;
            default:
                ku.logError("未知的转换："+ti.getString(TRANS_RULE));
                break;
            }
        }
    }
    /**
    *  <br/>
    * @author jingma
    * @param object
    * @param ti
    * @param outputRow
    */
    private void transToTxt(Object object, JSONObject ti, Object[] outputRow) {
        List<String> efs = getEfs(ti);
        JSONObject dataObj = ti.getJSONObject(TRANS_RULE_DATA);
        String fbf = dataObj.getString(TRANS_RULE_DATA_FBF);
        String fgf = dataObj.getString(TRANS_RULE_DATA_FGF);
        StringBuffer result = new StringBuffer();
        for(ValueMetaInterface om:data.outputRowMeta.getValueMetaList()){
            if(!ti.getString(TRANS_FIELD).equalsIgnoreCase(om.getName())&&!efs.contains(om.getName())){
                result.append(fbf+outputRow[getFieldIndex(om.getName())]+fbf+fgf);
            }
        }
        outputRow[getFieldIndex(ti)] = result.substring(0, result.length()-fgf.length());
    }
    /**
    *  <br/>
    * @author jingma
    * @param object
    * @param ti
    * @param outputRow
    */
    private void transToJson(Object object, JSONObject ti, Object[] outputRow) {
        List<String> efs = getEfs(ti);
        JSONObject resultObj = new JSONObject();
        for(ValueMetaInterface om:data.outputRowMeta.getValueMetaList()){
            if(!ti.getString(TRANS_FIELD).equalsIgnoreCase(om.getName())&&!efs.contains(om.getName())){
                resultObj.put(om.getName().toLowerCase(), outputRow[getFieldIndex(om.getName())]);
            }
        }
        String result = resultObj.toJSONString();
        outputRow[getFieldIndex(ti)] = result;
    }
    /**
    * 获取排除字段集合 <br/>
    * @author jingma
    * @param ti
    * @return
    */
    public List<String> getEfs(JSONObject ti) {
        List<String> efs = efsMap.get(ti.toJSONString());
        if(efs==null){
            efs = Arrays.asList(ti.getJSONObject(TRANS_RULE_DATA).
                    getJSONArray(TRANS_RULE_DATA_EXCLUDES_FIELDS).toArray(new String[]{}));
            efsMap.put(ti.toJSONString(), efs);
        }
        return efs;
    }
    /**
    *  <br/>
    * @author jingma
    * @param object
    * @param ti
    * @param outputRow
    */
    private void transDict(Object object, JSONObject ti,
            Object[] outputRow) {
        if(object==null){
            return;
        }
        String result = Dict.dictValue(ti.getString(TRANS_RULE_DATA), object.toString());
        outputRow[getFieldIndex(ti)] = result;
    }
    /**
    *  <br/>
    * @author jingma
    * @param object
    * @param ti
    * @param outputRow
    */
    private void transSfzh(Object object, JSONObject ti,
            Object[] outputRow) {
        if(object==null){
            return;
        }
        String result = SfzhUtil.conver15CardTo18(object.toString());
        if(result==null){
            result = object.toString();
        }
        outputRow[getFieldIndex(ti)] = result;
    }
    /**
    *  <br/>
    * @author jingma
    * @param object
    * @param ti
    * @param outputRow
    */
    private void transDateFormat(Object object, JSONObject ti,
            Object[] outputRow) {
        if(object==null){
            return;
        }
        Date date = DateUtil.parseDate(object.toString());
        if(date==null){
            outputRow[getFieldIndex(ti)] = object;
        }else{
            outputRow[getFieldIndex(ti)] = DateUtil.doFormatDate(date, ti.getString(TRANS_RULE_DATA));
        }
    }
    /**
    *  <br/>
    * @author jingma
    * @param ti
    * @return
    */
    private int getFieldIndex(JSONObject ti) {
        if(ti.containsKey(RESULT_LATER)&&StringUtils.isNotBlank(ti.getString(RESULT_LATER))){
            return getFieldIndex(ti.getString(TRANS_FIELD)+ti.getString(RESULT_LATER));
        }else{
            return getFieldIndex(ti.getString(TRANS_FIELD));
        }
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
        JSONArray TransInfo = new JSONArray();
        
        //时间格式
        JSONObject json = new JSONObject();
        json.put(TRANS_FIELD, "test_date");
        json.put(TRANS_RULE, RULE_DATE_FORMAT);
        json.put(TRANS_RULE_DATA, "yyyyMMddhhmmss");
        TransInfo.add(json);
        //身份证格式校验
        json = new JSONObject();
        json.put(TRANS_FIELD, "test_sfzh");
        json.put(TRANS_RULE, RULE_SFZH);
        json.put(TRANS_RULE_DATA, "");
        TransInfo.add(json);
        //从数据库获取数据
        json = new JSONObject();
        json.put(TRANS_FIELD, "test_dict");
        json.put(TRANS_RULE, RULE_DICT);
        json.put(TRANS_RULE_DATA, "WHETHER");
        //有则新加字段，否则直接修改来源值
        json.put(RESULT_LATER, "_MC");
        TransInfo.add(json);
        //转换为JSON
        json = new JSONObject();
        json.put(TRANS_FIELD, "test_tojson");
        json.put(TRANS_RULE, RULE_TOJSON);
        JSONObject data = new JSONObject();
        JSONArray dataArr = new JSONArray();
        dataArr.add("TEST_TOTXT");
        dataArr.add("VALIDATE_INFO");
        data.put(TRANS_RULE_DATA_EXCLUDES_FIELDS, dataArr);
        json.put(TRANS_RULE_DATA, data);
        TransInfo.add(json);
        //转换为TXT
        json = new JSONObject();
        json.put(TRANS_FIELD, "test_totxt");
        json.put(TRANS_RULE, RULE_TOJSON);
        data = new JSONObject();
        dataArr = new JSONArray();
        dataArr.add("TEST_TOJSON");
        dataArr.add("VALIDATE_INFO");
        data.put(TRANS_RULE_DATA_EXCLUDES_FIELDS, dataArr);
        data.put(TRANS_RULE_DATA_FGF, "|");
        data.put(TRANS_RULE_DATA_FBF, "");
        json.put(TRANS_RULE_DATA, data);
        TransInfo.add(json);
        
        params.put(TRANS_INFO, TransInfo);
        return JSON.toJSONString(params, true);
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        
        JSONArray transInfo = configInfo.getJSONArray(TRANS_INFO);
        for(JSONObject ti:transInfo.toArray(new JSONObject[]{})){
            //将转换结果赋予新字段的，添加新字段
            if(ti.containsKey(RESULT_LATER)&&StringUtils.isNotBlank(ti.getString(RESULT_LATER))){
                addField(r,ti.getString(TRANS_FIELD)+ti.getString(RESULT_LATER),
                        ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,ti.getString(TRANS_FIELD)+"名称");
            }
            if(RULE_TOJSON.equals(ti.getString(TRANS_RULE))){
                //转换为JSON串
                addField(r,ti.getString(TRANS_FIELD),
                        ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,ti.getString(TRANS_FIELD));
            }
            if(RULE_TOTXT.equals(ti.getString(TRANS_RULE))){
                //转换为JSON串
                addField(r,ti.getString(TRANS_FIELD),
                        ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,ti.getString(TRANS_FIELD));
            }
        }
    }
}
