/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.bdhc;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import cn.benma666.myutils.JsonResult;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 比对核查-全量比对-资源过滤<br/>
 * date: 2016年6月29日 <br/>
 * @author jingma
 * @version 
 */
public class QlbdZygl extends EasyExpandRunBase{
    
    /**
    * 具体处理每一行数据
     * @return 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
    */
    @Override
    protected JsonResult disposeRow(Object[] outputRow) throws Exception{
        RowMetaInterface irm = ku.getInputRowMeta();
        JSONObject hm = new JSONObject(100);
        for(int i=0;i<irm.size();i++){
            ValueMetaInterface vm = irm.getValueMeta(i);
            hm.put(vm.getName().toLowerCase(), inputRow[ku.getInputRowMeta().indexOfValue(vm.getName())]);
        }
        if(BdhcUtil.isZy(getVariavle("HCFS"), getVariavle("ZYLB"), hm)){
            //该号码需要比对本资源
            outputRow = new Object[1];
            outputRow[getFieldIndex("HMOBJ")] = hm;
            ku.putRow(data.outputRowMeta, outputRow);
        }
        return success("99");
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
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, 
            StepMeta nextStep, VariableSpace space) {
        //将转换好的号码JSON对象传到下一步
        r.clear();
        addField(r,"HMOBJ",ValueMeta.TYPE_NONE,
                ValueMeta.TRIM_TYPE_NONE,origin,"号码对象");
    }
}
