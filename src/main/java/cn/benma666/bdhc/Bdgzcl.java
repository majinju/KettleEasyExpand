/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.bdhc;

import java.util.List;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import cn.benma666.constants.UtilConst;
import cn.benma666.exception.FieldRuleVerifyException;
import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import cn.benma666.myutils.FieldRuleTrans;
import cn.benma666.myutils.FieldRuleVerify;
import cn.benma666.myutils.JsonResult;
import cn.benma666.myutils.StringUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 比对核查-比对规则处理<br/>
 * @author jingma
 * @version 
 */
public class Bdgzcl extends EasyExpandRunBase{
    
    /**
    * 具体处理每一行数据
     * @return 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
    */
    @Override
    protected JsonResult disposeRow(Object[] outputRow) throws Exception{
        JSONObject hm = (JSONObject)outputRow[getFieldIndex("HMOBJ")];
        JSONObject zy = (JSONObject)outputRow[getFieldIndex("ZYOBJ")];
        outputRow[getFieldIndex("HMOBJ")] = "";
        outputRow[getFieldIndex("ZYOBJ")] = "";
        outputRow[getFieldIndex("YXX")] = bdgz(hm,zy,outputRow)?
                UtilConst.WHETHER_TRUE:UtilConst.WHETHER_FALSE;
        return success("ok");
    }
    /**
    * 比对规则 <br/>
    * @author jingma
    * @param hm 号码信息
    * @param zy 资源信息
    * @param outputRow 输出数据数组，字段的jg.开头的字段及对应该数据
    * @return 是否推送
    */
    private boolean bdgz(JSONObject hm, JSONObject zy,Object[] outputRow) {
        List<JSONObject> xggzList = BdhcUtil.bdhcDb.find("bdhc.selectHmxggz", hm);
        if(xggzList.size()==0){
            //没有匹配的规则
            return true;
        }
        JSONObject ppgz = gzpp(hm, outputRow, xggzList);
        outputRow[getFieldIndex("PPGZ")] = StringUtil.join(ppgz.values(), ",");
        boolean isSc = zhgz(hm, outputRow, xggzList, ppgz);
        return isSc;
    }
    /**
    * 转换规则，字段属性值会按规则进行转换<br/>
    * 从优先级低到高进行转换，高等级转换是建立在等级转换的基础上的，高等级转换会覆盖低等级转换
    * @author jingma
    * @param hm
    * @param outputRow
    * @param xggzList
    * @param ppgz
    * @return 是否输出
    */
    public boolean zhgz(JSONObject hm, Object[] outputRow,
            List<JSONObject> xggzList, JSONObject ppgz) {
        boolean isSc = true;
        for(int i=xggzList.size()-1;i>-1;i--){
            JSONObject xggz = xggzList.get(i);
            if(!ppgz.containsValue(xggz.getString("id"))){
                //不属于匹配上的规则跳过
                continue;
            }
            //是否输出判断
            if(UtilConst.WHETHER_FALSE.equals(xggz.getString("sfsc"))){
                isSc = false;
            }else if(UtilConst.WHETHER_TRUE.equals(xggz.getString("sfsc"))){
                isSc = true;
            }

            //字段修改
            if(StringUtil.isBlank(xggz.getString("sjzd"))){
                //没有配置字段信息
                continue;
            }
            String value = getValByGzzd(hm, outputRow, xggz);
            value = FieldRuleTrans.ruleTrans(value, xggz);
            if(xggz.getString("sjzd").startsWith("jg.")){
                outputRow[getFieldIndex(xggz.getString("sjzd").substring(3))]=value;
            }else{
                //号码对象的修改已经无意义，无后续使用
                hm.put(xggz.getString("sjzd").substring(3),value);
            }
        }
        return isSc;
    }
    /**
    * 规则匹配 <br/>
    * 筛选出匹配的规则，每个级别只匹配一条规则
    * @author jingma
    * @param hm
    * @param outputRow
    * @param xggzList
    * @return 匹配成功的规则
    */
    public JSONObject gzpp(JSONObject hm, Object[] outputRow,
            List<JSONObject> xggzList) {
        //当前规则
        String dqgz = xggzList.get(0).getString("id");
        //当前规则等级
        String dqgzdj = xggzList.get(0).getString("dj");
        //当前规则有效
        boolean dqgzyx = true;
        //匹配规则<等级，规则主键>
        JSONObject ppgz = new JSONObject(true);
        for(JSONObject xggz:xggzList){
            if((!dqgzyx&&dqgz.equals(xggz.getString("id")))){
                //（当前规则无效且属于同一规则）
                continue;
            }
            if(!dqgz.equals(xggz.getString("id"))){
                //下一条规则
                if(dqgzyx&&!ppgz.containsKey(dqgzdj)){
                    //上一条规则匹配成功，且该等级第一条匹配规则，同等级其他规则跳过
                    ppgz.put(dqgzdj, dqgz);
                }
                
                //信息设置为新规则
                dqgz = xggz.getString("id");
                dqgzdj = xggz.getString("dj");
                dqgzyx = true;
            }
            if(ppgz.containsKey(dqgzdj)||StringUtil.isBlank(xggz.getString("sjzd"))){
                //该等级已经有匹配的规则或没有配置字段信息
                continue;
            }
            //获取对应的字段值
            String value = getValByGzzd(hm, outputRow, xggz);
            try {
                FieldRuleVerify.ruleVerify(value, xggz);
            } catch (FieldRuleVerifyException e) {
                dqgzyx = false;
            } catch (Exception e) {
                ku.logError("当前验证出错："+xggz+",hm:"+hm,e);
            }
        }
        //最后一条规则处理
        if(dqgzyx&&!ppgz.containsKey(dqgzdj)){
            //上一条规则匹配成功，且该等级第一条匹配规则，同等级其他规则跳过
            ppgz.put(dqgzdj, dqgz);
        }
        return ppgz;
    }
    public String getValByGzzd(JSONObject hm, Object[] outputRow,
            JSONObject xggz) {
        String value = null;
        if(xggz.getString("sjzd").startsWith("jg.")){
            Object obj = outputRow[getFieldIndex(xggz.getString("sjzd").substring(3))];
            value = obj==null?"":obj+"";
        }else{
            value = hm.getString(xggz.getString("sjzd").substring(3));
        }
        return value;
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
        ku.logBasic("数据处理结束");
    }

    /**
     * 
     * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#getDefaultConfigInfo(org.pentaho.di.trans.TransMeta, java.lang.String)
     */
     @Override
     public String getDefaultConfigInfo(TransMeta transMeta, String stepName) throws Exception{
        //创建一个JSON对象，用于构建配置对象，避免直接拼字符串构建JSON字符串
        JSONObject params = new JSONObject();
        //返回格式化后的默认JSON配置参数，供使用者方便快捷的修改配置
        return JSON.toJSONString(params, true);
    }
    
    @SuppressWarnings("deprecation")
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, 
            VariableSpace space) {
        //修改对象的字段类型为字符串，不然后续步骤会产生些问题。
        r.getValueMeta(r.indexOfValue("HMOBJ")).setType(ValueMeta.TYPE_STRING);
        r.getValueMeta(r.indexOfValue("ZYOBJ")).setType(ValueMeta.TYPE_STRING);
        //添加输出到下一步的字段
        addField(r,"YXX",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"有效性");
        addField(r,"PPGZ",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"匹配规则");
    }
}
