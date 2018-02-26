/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettle.easyexpand;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
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
import cn.benma666.myutils.StringUtil;

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
    private static final String TRANS_FIELD = "转换字段";
    private static final String TRANS_RULE = "转换规则";
    private static final String RESULT_LATER = "结果后缀";
    private static final String TRANS_RULE_DATA = "转换规则数据";

    //规则
    private static final String RULE_REPLACE = "字符串替换";
    private static final String RULE_DATE_FORMAT = "时间格式";
    private static final String RULE_SFZH = "身份证格式";
    private static final String RULE_DICT = "字典";
    private static final String RULE_TOJSON = "合并为JSON";
    private static final String RULE_TOTXT = "合并为TXT";
    
    private static final String TRANS_HBJLS = "合并记录数";
    private static final String TRANS_GROUP_FIELDS = "分组字段";
    
    private static final String TRANS_RULE_DATA_EXCLUDES_FIELDS = "排除的字段";
    private static final String TRANS_RULE_DATA_FGF = "分隔符";
    private static final String TRANS_RULE_DATA_FBF = "封闭符";
    private static final String TRANS_RULE_DATA_JLFGF = "记录分隔符";
    
    /**
     * 排出字段集合的Map
     */
    private Map<String,List<String>> efsMap = new HashMap<String,List<String>>();
    /**
     * 记录分隔符Map
     */
    private Map<String,String> jlfgfMap = new HashMap<String,String>();
    
    /**
     * 已经合并记录数
     */
    private int hbjlCount = 0;
    /**
     * 转换为txt合并记录
     */
    private StringBuffer toTxtHbjl = new StringBuffer();
    /**
     * 转换为json合并记录
     */
    private JSONArray toJsonHbjl = new JSONArray();
    /**
     * 分组字段拼接出的key
     */
    private String groupKey = null; 
    /**
     * 上一个输出集
     */
    private Object[] upOutputRow = null;

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
        	//存在分组字段则需要合并记录
            if(isHbjl()&&upOutputRow!=null){
                //没有输入了，将之前合并的推送出去
                ku.putRow(data.outputRowMeta, upOutputRow);
            }
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
        
        if(isHbjl()){
            StringBuffer currGroupKey = getGroupKey(outputRow);
            if(groupKey!=null&&(!currGroupKey.toString().equals(groupKey)||hbjlCount==configInfo.getIntValue(TRANS_HBJLS))){
                //分组key变化或达到合并记录数则推送上一步的结果
                ku.putRow(data.outputRowMeta, upOutputRow);
                hbjlCount = 0;
                groupKey = null;
                toJsonHbjl =  new JSONArray();
                toTxtHbjl =  new StringBuffer();
                upOutputRow = null;
            }
            if(groupKey==null){
            	groupKey = currGroupKey.toString();
            }
            outputRow[getFieldIndex("GROUP_KEY")] = groupKey;
        	hbjlCount++;
        }
        
        for(JSONObject ti:configInfo.getJSONArray(TRANS_INFO).toArray(new JSONObject[]{})){
            String txt;
			JSONObject json;
			switch (ti.getString(TRANS_RULE)) {
            case RULE_REPLACE:
                transReplace(outputRow, ti);
                break;
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
            	json = transToJson(outputRow[getFieldIndex(ti.getString(TRANS_FIELD))], ti,outputRow);
                if(isHbjl()){
                	toJsonHbjl.add(json);
                    outputRow[getFieldIndex(ti)] = toJsonHbjl.toJSONString();
                }
                break;
            case RULE_TOTXT:
            	txt = transToTxt(outputRow[getFieldIndex(ti.getString(TRANS_FIELD))], ti,outputRow);
                if(isHbjl()){
                	toTxtHbjl.append(getJlFGF(ti)+txt);
                    outputRow[getFieldIndex(ti)] = toTxtHbjl.substring(getJlFGF(ti).length());
                }
                break;
            default:
                ku.logError("未知的转换："+ti.getString(TRANS_RULE));
                break;
            }
        }
        if(!isHbjl()){
            ku.putRow(data.outputRowMeta, outputRow);
        }else{
            upOutputRow = outputRow;
        }
        return true;
    }
	/**
    * 字符串替换 <br/>
    * @author jingma
    * @param outputRow
    * @param ti
    */
    private void transReplace(Object[] outputRow, JSONObject ti) {
        String[] dataArr = ti.getString(TRANS_RULE_DATA).split("->");
        String toStr = dataArr.length==1?"":dataArr[1];
        String field = ti.getString(TRANS_FIELD);
        if(StringUtil.isBlank(field)){
            //字段为空代表作用于全部字段
            for(ValueMetaInterface vm:data.outputRowMeta.getValueMetaList()){
                //类型是字符串的字段,且不为空
                if(vm.getType()==ValueMetaInterface.TYPE_STRING&&outputRow[getFieldIndex(vm.getName())]!=null){
                    outputRow[getFieldIndex(vm.getName())] = outputRow[getFieldIndex(vm.getName())].toString()
                            .replace(dataArr[0], toStr);
                }
            }
        }else{
            outputRow[getFieldIndex(field)] = outputRow[getFieldIndex(field)].toString()
                    .replace(dataArr[0], toStr);
        }
    }
    /**
	 * @return 是否合并记录
	 */
	private boolean isHbjl() {
		return configInfo.containsKey(TRANS_HBJLS)&&configInfo.getIntValue(TRANS_HBJLS)>1;
	}
	/**
	 * @param outputRow
	 * @return 分组字段组成的key
	 */
	private StringBuffer getGroupKey(Object[] outputRow) {
		StringBuffer currGroupKey = new StringBuffer();
		if(!configInfo.containsKey(TRANS_GROUP_FIELDS)){
			currGroupKey.append("NoGroup");
		}else{
	        for(String str : configInfo.getJSONArray(TRANS_GROUP_FIELDS).toArray(new String[]{})){
	        	currGroupKey.append(outputRow[getFieldIndex(str)]+"_");
	    	}
		}
		return currGroupKey;
	}
	/**
	 * @param ti
	 * @return 记录间分隔符
	 */
	private String getJlFGF(JSONObject ti) {
		String jlfgf = jlfgfMap.get(ti.toString());
		if(jlfgf == null){
			jlfgf = ti.getJSONObject(TRANS_RULE_DATA).getString(TRANS_RULE_DATA_JLFGF);
			jlfgfMap.put(ti.toString(), jlfgf);
		}
		return jlfgf;
	}
    /**
    *  <br/>
    * @author jingma
    * @param object
    * @param ti
    * @param outputRow
     * @return 
    */
    private String transToTxt(Object object, JSONObject ti, Object[] outputRow) {
        List<String> efs = getEfs(ti);
        JSONObject dataObj = ti.getJSONObject(TRANS_RULE_DATA);
        String fgf = dataObj.getString(TRANS_RULE_DATA_FGF);
        String fbf = dataObj.getString(TRANS_RULE_DATA_FBF);
        StringBuffer buff = new StringBuffer();
        for(ValueMetaInterface om:data.outputRowMeta.getValueMetaList()){
            if(!ti.getString(TRANS_FIELD).equalsIgnoreCase(om.getName())&&!efs.contains(om.getName())){
                Object obj = outputRow[getFieldIndex(om.getName())];
                if(obj==null){
                    obj = "";
                }
            	buff.append(fbf+obj+fbf+fgf);
            }
        }
        String result = buff.substring(0, buff.length()-fgf.length());
        outputRow[getFieldIndex(ti)] = result;
        return result;
    }
    /**
    *  <br/>
    * @author jingma
    * @param object
    * @param ti
    * @param outputRow
     * @return 
    */
    private JSONObject transToJson(Object object, JSONObject ti, Object[] outputRow) {
        List<String> efs = getEfs(ti);
        JSONObject resultObj = new JSONObject();
        for(ValueMetaInterface om:data.outputRowMeta.getValueMetaList()){
            if(!ti.getString(TRANS_FIELD).equalsIgnoreCase(om.getName())&&!efs.contains(om.getName())){
                resultObj.put(om.getName().toUpperCase(), outputRow[getFieldIndex(om.getName())]);
            }
        }
        String result = resultObj.toJSONString();
        outputRow[getFieldIndex(ti)] = result;
        return resultObj;
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
        String date = DateUtil.dateToStr14(object);
        if(date==null){
            outputRow[getFieldIndex(ti)] = object;
        }else{
            outputRow[getFieldIndex(ti)] = DateUtil.doFormatDate(DateUtil.parseDate(date), ti.getString(TRANS_RULE_DATA));
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
        json.put(TRANS_RULE_DATA, "yyyyMMddHHmmss");
        TransInfo.add(json);
        //字符串替换
        json = new JSONObject();
        json.put(TRANS_FIELD, "test_str");
        json.put(TRANS_RULE, RULE_REPLACE);
        json.put(TRANS_RULE_DATA, "\u0000->");
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
        json.put(TRANS_FIELD, "TEST_TOJOSN");
        json.put(TRANS_RULE, RULE_TOJSON);
        JSONObject data = new JSONObject();
        JSONArray dataArr = new JSONArray();
        dataArr.add("TEST_TOJOSN");
        dataArr.add("TEST_TOTXT");
        dataArr.add("VALIDATE_INFO");
        dataArr.add("GROUP_KEY");
        data.put(TRANS_RULE_DATA_EXCLUDES_FIELDS, dataArr);
        json.put(TRANS_RULE_DATA, data);
        TransInfo.add(json);
        //转换为TXT
        json = new JSONObject();
        json.put(TRANS_FIELD, "TEST_TOTXT");
        json.put(TRANS_RULE, RULE_TOTXT);
        data = new JSONObject();
        dataArr = new JSONArray();
        dataArr.add("TEST_TOJOSN");
        dataArr.add("TEST_TOTXT");
        dataArr.add("VALIDATE_INFO");
        dataArr.add("GROUP_KEY");
        data.put(TRANS_RULE_DATA_EXCLUDES_FIELDS, dataArr);
        data.put(TRANS_RULE_DATA_FGF, "\u0003");
        data.put(TRANS_RULE_DATA_JLFGF, "\u0004");
        data.put(TRANS_RULE_DATA_FBF, "");
        json.put(TRANS_RULE_DATA, data);
        TransInfo.add(json);
        
        params.put(TRANS_INFO, TransInfo);
        
        dataArr = new JSONArray();
        dataArr.add("XM");
        params.put(TRANS_GROUP_FIELDS, dataArr);
        params.put(TRANS_HBJLS, 100);
        
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
        if(isHbjl()){
            addField(r,"GROUP_KEY",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"分组字段拼接出的key");
        }
    }
}
