/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettle.easyexpand;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import cn.benma666.kettle.mytuils.Db;
import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import cn.benma666.myutils.DateUtil;
import cn.benma666.myutils.SfzhUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 数据校验<br/>
 * date: 2016年6月29日 <br/>
 * @author jingma
 * @version 
 */
public class DataValidate extends EasyExpandRunBase{
    //字段
    private static final String FIELD_VALIDATE_INFO = "VALIDATE_INFO";
    //结果处理
    private static final String RESULT_DISPOSE = "结果处理方式";
    private static final String RESULT_VALIDATE_INFO = "验证信息";
    private static final String RESULT_CONTINUE = "跳过";
    //参数
    private static final String VALIDATE_VAL = "值";
    private static final String VALIDATE_INFO = "验证信息";
    private static final String VALIDATE_FIELD = "验证字段";
    private static final String VALIDATE_RULE = "验证规则";
    private static final String VALIDATE_RULE_DATA = "验证规则数据";
    private static final String VALIDATE_RULE_DATA_SQL = "获取验证规则数据的SQL";
    private static final String VALIDATE_RULE_DATA_ARR = "验证规则数据的数组";

    private static final String RULE_NOT_EMPTY = "不为空";
    /**
    * 规则-时间格式
    */
    private static final String RULE_DATE_FORMAT = "时间格式";
    /**
    * 规则-身份证格式
    */
    private static final String RULE_SFZH = "身份证格式";
    /**
    * 规则-包含
    */
    private static final String RULE_IN = "包含";
    /**
    * 规则-不包含
    */
    private static final String RULE_NOT_IN = "不包含";
    /**
    * 规则-匹配
    */
    private static final String RULE_LIKE = "匹配";
    /**
    * 规则-不匹配
    */
    private static final String RULE_NOT_LIKE = "不匹配";
    
    /**
    * 需要的数据缓存
    */
    private static Map<String,List<String>> dataListCache = new HashMap<String, List<String>>();

    /**
    * 开始处理每一行数据 <br/>
    * @author jingma
    * @return 
    * @throws KettleException 
    */
    public boolean run() throws Exception{
        Object[] r = ku.getRow(); // get row, blocks when needed!
        if (r == null) // no more input to be expected...
        {
            end();
            ku.setOutputDone();
            return false;
        }
        if (ku.first) {
            data.outputRowMeta = (RowMetaInterface) ku.getInputRowMeta().clone();
            getFields(data.outputRowMeta, ku.getStepname(), null, null, ku);
            ku.first = false;
            init();
        }
        //创建输出记录
        Object[] outputRow = RowDataUtil.createResizedCopy( r, data.outputRowMeta.size() );
        //验证信息
        JSONArray validateInfo = new JSONArray();
        for(JSONObject vi:configInfo.getJSONArray(VALIDATE_INFO).toArray(new JSONObject[]{})){
            switch (vi.getString(VALIDATE_RULE)) {
            case RULE_NOT_EMPTY:
                validateNotEmpty(outputRow[getFieldIndex(vi.getString(VALIDATE_FIELD))],vi,validateInfo);
                break;
            case RULE_DATE_FORMAT:
                validateDateFormat(outputRow[getFieldIndex(vi.getString(VALIDATE_FIELD))],vi,validateInfo);
                break;
            case RULE_SFZH:
                validateSfzh(outputRow[getFieldIndex(vi.getString(VALIDATE_FIELD))],vi,validateInfo);
                break;
            case RULE_IN:
                validateIn(outputRow[getFieldIndex(vi.getString(VALIDATE_FIELD))],vi,validateInfo);
                break;
            case RULE_NOT_IN:
                validateNotIn(outputRow[getFieldIndex(vi.getString(VALIDATE_FIELD))],vi,validateInfo);
                break;
            case RULE_LIKE:
                validateLike(outputRow[getFieldIndex(vi.getString(VALIDATE_FIELD))],vi,validateInfo);
                break;
            case RULE_NOT_LIKE:
                validateNotLike(outputRow[getFieldIndex(vi.getString(VALIDATE_FIELD))],vi,validateInfo);
                break;
            default:
                ku.logError("未知的验证："+vi.getString(VALIDATE_RULE));
                break;
            }
        }
        if(validateInfo.size()>0){
            if(RESULT_VALIDATE_INFO.equals(configInfo.getString(RESULT_VALIDATE_INFO))){
                outputRow[getFieldIndex(FIELD_VALIDATE_INFO)] = validateInfo.toJSONString();
                //将该记录设置到下一步骤的读取序列中
                ku.putRow(data.outputRowMeta, outputRow); // copy row to possible alternate rowset(s)
            }else if(RESULT_CONTINUE.equals(configInfo.getString(RESULT_VALIDATE_INFO))){
                //跳过
            }
        }else{
            //将该记录设置到下一步骤的读取序列中
            ku.putRow(data.outputRowMeta, outputRow); // copy row to possible alternate rowset(s)
        }
    
        return true;
    }
    
    /**
    *  <br/>
    * @author jingma
    * @param object
    * @param vi
    * @param validateInfo
    */
    private boolean validateNotEmpty(Object object, JSONObject vi,
            JSONArray validateInfo) {
        boolean result = true;
        if(object==null){
            result = false;
        }else{
            result = StringUtils.isNotBlank(object.toString());
        }
        if(!result){
            vi.put(VALIDATE_VAL, object.toString());
            validateInfo.add(vi);
        }
        return result;
    }

    /**
    * 不匹配 <br/>
    * @author jingma
    * @param object
    * @param vi
    * @param validateInfo
    */
    private boolean validateNotLike(Object object, JSONObject vi, JSONArray validateInfo) {
        if(object==null){
            return true;
        }
        List<String> dataList = getValidateData(vi);
        boolean result = true;
        for(String val:dataList){
            if(object.toString().indexOf(val)>-1){
                result = false;
                break;
            }
        }
        if(!result){
            vi.put(VALIDATE_VAL, object.toString());
            validateInfo.add(vi);
        }
        return result;
    }
    /**
    * 匹配 <br/>
    * @author jingma
    * @param object
    * @param vi
    * @param validateInfo
    */
    private boolean validateLike(Object object, JSONObject vi, JSONArray validateInfo) {
        if(object==null){
            return true;
        }
        List<String> dataList = getValidateData(vi);
        boolean result = false;
        for(String val:dataList){
            if(object.toString().indexOf(val)>-1){
                result = true;
                break;
            }
        }
        if(!result){
            vi.put(VALIDATE_VAL, object.toString());
            validateInfo.add(vi);
        }
        return result;
    }
    /**
    * 不包含 <br/>
    * @author jingma
    * @param object
    * @param vi
    * @param validateInfo
    */
    private boolean validateNotIn(Object object, JSONObject vi, JSONArray validateInfo) {
        if(object==null){
            return true;
        }
        List<String> dataList = getValidateData(vi);
        boolean result = !dataList.contains(object.toString());
        if(!result){
            vi.put(VALIDATE_VAL, object.toString());
            validateInfo.add(vi);
        }
        return result;
    }
    /**
    * 包含 <br/>
    * @author jingma
    * @param object
    * @param vi
    * @param validateInfo
    */
    private boolean validateIn(Object object, JSONObject vi, JSONArray validateInfo) {
        if(object==null){
            return true;
        }
        List<String> dataList = getValidateData(vi);
        boolean result = dataList.contains(object.toString());
        if(!result){
            vi.put(VALIDATE_VAL, object.toString());
            validateInfo.add(vi);
        }
        return result;
    }
    /**
    * 获取验证数据 <br/>
    * @author jingma
    * @param vi
    * @return
    */
    private List<String> getValidateData(JSONObject vi) {
        List<String> result = dataListCache.get(vi.toJSONString());
        if(result!=null){
            return result;
        }
        if(!vi.containsKey(VALIDATE_RULE_DATA)){
            result = new ArrayList<String>();
        }
        JSONObject dataObj = vi.getJSONObject(VALIDATE_RULE_DATA);
        if(dataObj.containsKey(VALIDATE_RULE_DATA_ARR)){
            result = Arrays.asList(dataObj.getJSONArray(VALIDATE_RULE_DATA_ARR).toArray(new String[]{}));
        }else{
            String[] sqls = parseSqlExp(dataObj.getString(VALIDATE_RULE_DATA_SQL));
            result = new ArrayList<String>(Db.use(sqls[1]).findMap("code",sqls[0]).keySet());
        }
        ku.logBasic(vi+","+JSON.toJSONString(result));
        dataListCache.put(vi.toJSONString(), result);
        return result;
    }
    /**
    *  <br/>
    * @author jingma
    * @param sqlExp
    * @return
    */
    private String[] parseSqlExp(String sqlExp) {
        String[] result = new String[2];
        String[] exps = sqlExp.split(";");
        result[0] = exps[0];
        if(exps.length==1||exps[1].length()<4){
            result[1] = "kettle";
        }else{
            result[1] = exps[1].substring(3);
        }
        return result;
    }
    /**
    * 身份证号码验证 <br/>
    * @author jingma
    * @param object
    * @param vi
    * @param validateInfo
    */
    private boolean validateSfzh(Object object, JSONObject vi,
            JSONArray validateInfo) {
        if(object==null){
            return true;
        }
        boolean result = SfzhUtil.validateCard(object.toString());
        if(!result){
            vi.put(VALIDATE_VAL, object.toString());
            validateInfo.add(vi);
        }
        return result;
    }
    /**
    * 验证时间格式 <br/>
    * @author jingma
    * @param dateStr
    * @param validateInfo 
    * @param vi 
     * @return 
    */
    private boolean validateDateFormat(Object dateStr, JSONObject vi, JSONArray validateInfo) {
        if(dateStr==null){
            return true;
        }
        Date date = DateUtil.parseDate(dateStr.toString());
        if(date==null){
            vi.put(VALIDATE_VAL, dateStr);
            validateInfo.add(vi);
            return false;
        }else{
            return true;
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
        params.put(RESULT_DISPOSE, RESULT_VALIDATE_INFO);
        JSONArray validateInfo = new JSONArray();
        
        //时间格式校验
        JSONObject json = new JSONObject();
        json.put(VALIDATE_FIELD, "test_date");
        json.put(VALIDATE_RULE, RULE_DATE_FORMAT);
        json.put(VALIDATE_RULE_DATA, "");
        validateInfo.add(json);
        //身份证格式校验
        json = new JSONObject();
        json.put(VALIDATE_FIELD, "test_sfzh");
        json.put(VALIDATE_RULE, RULE_SFZH);
        json.put(VALIDATE_RULE_DATA, "");
        validateInfo.add(json);
        //从数据库获取数据
        json = new JSONObject();
        json.put(VALIDATE_FIELD, "test_in");
        json.put(VALIDATE_RULE, RULE_IN);
        JSONObject data = new JSONObject();
        data.put(VALIDATE_RULE_DATA_SQL, "select 1 as code from dual;ds=kettle");
        json.put(VALIDATE_RULE_DATA, data);
        validateInfo.add(json);
        //直接供验证数据
        json = new JSONObject();
        json.put(VALIDATE_FIELD, "test_not_in");
        json.put(VALIDATE_RULE, RULE_NOT_IN);
        JSONArray dataArr = new JSONArray();
        dataArr.add("1");
        dataArr.add("2");
        dataArr.add("3");
        data = new JSONObject();
        data.put(VALIDATE_RULE_DATA_ARR, dataArr);
        json.put(VALIDATE_RULE_DATA, data);
        validateInfo.add(json);
        //从数据库获取数据
        json = new JSONObject();
        json.put(VALIDATE_FIELD, "test_like");
        json.put(VALIDATE_RULE, RULE_LIKE);
        data = new JSONObject();
        data.put(VALIDATE_RULE_DATA_SQL, "select 1 as code from dual;ds=kettle");
        json.put(VALIDATE_RULE_DATA, data);
        validateInfo.add(json);
        //直接提供验证数据
        json = new JSONObject();
        json.put(VALIDATE_FIELD, "test_not_like");
        json.put(VALIDATE_RULE, RULE_NOT_LIKE);
        dataArr = new JSONArray();
        dataArr.add("1");
        dataArr.add("2");
        dataArr.add("3");
        data = new JSONObject();
        data.put(VALIDATE_RULE_DATA_ARR, dataArr);
        json.put(VALIDATE_RULE_DATA, data);
        validateInfo.add(json);
        
        params.put(VALIDATE_INFO, validateInfo);
        return JSON.toJSONString(params, true);
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        //添加输出到下一步的字段
        if(RESULT_VALIDATE_INFO.equals(configInfo.getString(RESULT_VALIDATE_INFO))){
            addField(r,FIELD_VALIDATE_INFO,ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"验证信息");
        }
    }
}
