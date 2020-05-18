/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.bdhc;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import cn.benma666.myutils.JsonResult;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 比对核查-比对规则处理<br/>
 * date: 2016年6月29日 <br/>
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
        if(bdgz(hm,zy)){
            return success("ok");
        }else{
            return success("99");
        }
    }
    /**
    * 比对规则 <br/>
    * @author jingma
    * @param hm 号码信息
    * @param zy 资源信息
    * @return 是否推送
    */
    private boolean bdgz(JSONObject hm, JSONObject zy) {
        //TODO 这个就比较复杂咯，后续慢慢编写吧
        return true;
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
        r.getValueMeta(r.indexOfValue("HMOBJ")).setType(ValueMeta.TYPE_STRING);
        r.getValueMeta(r.indexOfValue("ZYOBJ")).setType(ValueMeta.TYPE_STRING);
        //添加输出到下一步的字段
    }
}
