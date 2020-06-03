/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.bdhc;

import java.util.List;

import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import cn.benma666.iframe.DictManager;
import cn.benma666.kettle.mytuils.Db;
import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import cn.benma666.myutils.JsonResult;
import cn.benma666.myutils.TmplUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 比对核查-全量比对-号码比对<br/>
 * 注意性能优化，本系统考虑十亿级比对号码支持
 * date: 2016年6月29日 <br/>
 * @author jingma
 * @version 
 */
public class QlbdHmbd extends EasyExpandRunBase{
    /**
    * 资源数据载体
    */
    private cn.benma666.db.Db zydb;
    /**
    * 核查语句模板
    */
    private String hcyj;

    /**
    * 具体处理每一行数据
     * @return 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
    */
    @Override
    protected JsonResult disposeRow(Object[] outputRow) throws Exception{
        JSONObject hm = (JSONObject)outputRow[getFieldIndex("HMOBJ")];
        hm.put("hcfs", "02");
        List<JSONObject> list = zydb.find(TmplUtil.buildStrSql(hcyj, hm));
        if(list.size()>0){
            ku.logBasic(hm.getString("bdhm")+"比中数据量："+list.size());
        }
        for(JSONObject zy:list){
            Object[] or1 = RowDataUtil.createResizedCopy( outputRow, data.outputRowMeta.size() );
            or1[getFieldIndex("ZYOBJ")] = zy;
            or1[getFieldIndex("HMOBJ")] = hm.clone();
            ku.putRow(data.outputRowMeta, or1);
        }
        return success("99");
    }
    /**
    * 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#init()
    */
    @Override
    protected void init() {
        //资源类别
        DictManager.clearDict("SYS_BDHC_ZY");
        JSONObject zy = DictManager.zdObjByDmByCache("SYS_BDHC_ZY", getVariavle("ZYLB"));
        zydb = Db.use(zy.getString("sjzt"));
        hcyj = zy.getString("hcyj");
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
        //添加输出到下一步的字段
        addField(r,"ZYOBJ",ValueMeta.TYPE_NONE,
                ValueMeta.TRIM_TYPE_NONE,origin,"资源对象");
    }
}
