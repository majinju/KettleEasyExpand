/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.bdhc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import cn.benma666.exception.MyException;
import cn.benma666.kettle.mytuils.Db;
import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import cn.benma666.myutils.JsonResult;
import cn.benma666.myutils.StringUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 比对核查-资源预处理<br/>
 * date: 2016年6月29日 <br/>
 * @author jingma
 * @version 
 */
public class Zyycl extends EasyExpandRunBase{
    
    private static final String P_JGZDPZ = "结果字段配置";
    private static final String P_BLZDPZ = "变量字段配置";
    /**
    * 输入字段信息
    */
    public JSONObject inputField = new JSONObject();
    /**
    * 备用字段映射关系<字段名称，字段对象>
    */
    private Map<String, JSONObject> flagYsgx = new HashMap<String, JSONObject>();
    
    /**
    * 具体处理每一行数据
     * @return 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
    */
    @Override
    protected JsonResult disposeRow(Object[] outputRow) throws Exception{
        JSONObject hm = (JSONObject)inputRow[ku.getInputRowMeta().indexOfValue("HMOBJ")];
        JSONObject zy = (JSONObject)inputRow[ku.getInputRowMeta().indexOfValue("ZYOBJ")];
        //去重信息处理
        if(!zy.containsKey("qczd")){
            //没有设置去重字段时，设置去重字段默认值：资源类别+资源源库主键
            zy.put("qczd", getVariavle("ZYLB")+"#"+zy.getString("zyykzj"));
        }
        //合并号码与资源的去重字段
        hm.put("jg_qczd", hm.getString("hm_qcbz")+"||"+zy.getString("qczd"));
        //变量读取
        for(JSONObject zd:configInfo.getJSONArray(P_BLZDPZ).toArray(new JSONObject[]{})){
            hm.put(zd.getString("字段代码"), getVariavle(zd.getString("变量名称")));
        }

        //合并活动相关信息
        JSONObject hdxgxxJO = hbHdxgxx(zy);
        zy.put("hdxgxx", hdxgxxJO.toJSONString());
        //资源备用字段关系映射
        for(String xgxx:hdxgxxJO.keySet()){
            try {
                outputRow[getFieldIndex(flagYsgx.get(xgxx).getString("zddm"))] = hdxgxxJO.get(xgxx);
            } catch (Exception e1) {
                throw new MyException("备用字段映射出错："+flagYsgx+hdxgxxJO, e1);
            }
        }
        
        //复制资源信息到hm对象中便于后续处理
        for(Entry<String, Object> e:zy.entrySet()){
            hm.put("zy_"+e.getKey(), e.getValue());
        }
        
        //补全输出信息，需要输出到结果中
        for(JSONObject zd:configInfo.getJSONArray(P_JGZDPZ).toArray(new JSONObject[]{})){
            outputRow[getFieldIndex(zd.getString("字段代码"))] = hm.getString(zd.getString("值来源"));
        }
        
        ku.logDebug("输出结果:"+Arrays.toString(outputRow));
        ku.putRow(data.outputRowMeta, outputRow);
        return success("99");
    }
    /**
    * 合并活动相关信息 <br/>
    * @author jingma
    * @param outputRow
    * @return
    */
    public JSONObject hbHdxgxx(JSONObject zy) {
        String hdxgxxTxt = zy.getString("hdxgxx");
        JSONObject hdxgxxJO = new JSONObject();
        if(StringUtil.isNotBlank(hdxgxxTxt)){
            //字段1#t2#字段1的值#t1#字段2#t2#字段2的值#t1#字段3#t2#字段3的值
            hdxgxxTxt = "{\""+hdxgxxTxt.replace("#t2#", "\":\"")
                    .replace("#t1#", "\",\"").replace("\n", "")+"\"}";
            try {
                hdxgxxJO = JSON.parseObject(hdxgxxTxt);
            } catch (Exception e) {
                ku.logError("解析为JSON对象失败："+hdxgxxTxt, e);
                for(String z:hdxgxxTxt.split("#t1#")){
                    String[] sa = z.split("#t2#");
                    hdxgxxJO.put(sa[0], sa[1]);
                }
            }
        }
        for(String zd:zy.keySet()){
            if(zd.startsWith("xgxx_")){
                hdxgxxJO.put(zd.substring(5).toUpperCase(), zy.getString(zd));
            }
        }
        return hdxgxxJO;
    }
    /**
    * 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#init()
    */
    @Override
    protected void init() {
        initByzdgxys();
    }
    /**
    * 备用字段关系映射 <br/>
    * @author jingma
    */
    public void initByzdgxys() {
        //资源类别
        String zylb = getVariavle("ZYLB");
        //查询有效的字段映射
        List<JSONObject> zdysList = Db.use().find("bdhc.selectZyzdysByZylb", Db.buildMap(zylb));
        //查询未的字段映射
        List<JSONObject> zdwysList = Db.use().find("bdhc.selectZyzdwysByZylb", Db.buildMap(zylb));
        //字段名称的映射关系
        Map<String, JSONObject> zdmcMap = Db.listToMap(zdysList, "zdmc");
        //添加flag字段输出，实现自动映射
        JSONObject hdxgxx = hbHdxgxx((JSONObject)inputRow[ku.getInputRowMeta().indexOfValue("ZYOBJ")]);
        int xzzdIdx = 0;
        for(String xgxx:hdxgxx.keySet()){
            if(zdmcMap.containsKey(xgxx)){
                //存在映射关系
                flagYsgx.put(xgxx, zdmcMap.get(xgxx));
            }else{
                //不存在映射关系
                JSONObject sjzd = zdwysList.get(xzzdIdx++);
                flagYsgx.put(xgxx, sjzd);
                //自动添加资源的映射关系
                Db.use().update("bdhc.updateSjzdById", Db.buildMap(xgxx,
                        sjzd.getString("id")));
            }
        }
        ku.logDebug("字段映射："+flagYsgx+Arrays.toString(inputRow));
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
        //资源字段
        JSONArray blzdpz = new JSONArray();
        JSONObject zd = new JSONObject();
        zd.put("字段代码", "qczd");
        zd.put("字段名称", "去重字段");
        zd.put("变量名称", "qczd");
        blzdpz.add(zd);
        params.put(P_BLZDPZ, blzdpz);
        //资源字段
        JSONArray jgzdpz = new JSONArray();
        zd = new JSONObject();
        zd.put("字段代码", "qczd");
        zd.put("字段名称", "去重字段");
        zd.put("值来源", "qczd");
        jgzdpz.add(zd);
        params.put(P_JGZDPZ, jgzdpz);
        
        //返回格式化后的默认JSON配置参数，供使用者方便快捷的修改配置
        return JSON.toJSONString(params, true);
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        //添加输出到下一步的字段
        for(JSONObject zd:configInfo.getJSONArray(P_JGZDPZ).toArray(new JSONObject[]{})){
            addField(r,zd.getString("字段代码"),ValueMeta.TYPE_STRING,
                    ValueMeta.TRIM_TYPE_BOTH,origin,zd.getString("字段名称"));
        }
        //补充备用字段输出信息
        for(int i=1;i<41;i++){
            addField(data.outputRowMeta,"flag"+String.format("%02d", i),ValueMeta.TYPE_STRING,
                    ValueMeta.TRIM_TYPE_BOTH,ku.getStepname(),"备用信息"+String.format("%02d", i));
        }
    }
}
