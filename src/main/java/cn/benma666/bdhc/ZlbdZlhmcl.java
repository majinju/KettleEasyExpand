/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.bdhc;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import cn.benma666.myutils.JsonResult;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 比对核查-增量比对-增量号码处理<br/>
 * @author jingma
 * @version 
 */
public class ZlbdZlhmcl extends EasyExpandRunBase{
    
    /**
    * 具体处理每一行数据
     * @return 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
    */
    @Override
    protected JsonResult disposeRow(Object[] outputRow) throws Exception{
        JSONObject r = new JSONObject();
        RowMetaInterface irm = ku.getInputRowMeta();
        for(int i=0;i<irm.size();i++){
            ValueMetaInterface vm = irm.getValueMeta(i);
            r.put(vm.getName().toLowerCase(), outputRow[getFieldIndex(vm.getName())]);
        }
        ku.logDebug("新增比对号码："+r);
        //以后可以考虑将比对号码对应的全部号码存在value中，但因为比中毕竟是少数，所以暂时不考虑
        ZlbdHmbd.bhhmMap.put(r.getString(BdhcUtil.FIELD_HCZJLX)+"_"
                +r.getString(BdhcUtil.FIELD_HCZJHM), r);
        return success("完成");
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
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
    }
}
