/**
* Project Name:KettleUtil
* Date:2016年7月6日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettle.steps.easyexpand;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.job.Job;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import cn.benma666.iframe.BasicObject;
import cn.benma666.kettle.mytuils.KettleUtils;
import cn.benma666.myutils.JsonResult;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * kettleUtil插件扩展基础类 <br/>
 * 要运用于该插件的类必须继承这个类<br/>
 * date: 2016年7月6日 <br/>
 * @author jingma
 * @version 
 */
public abstract class EasyExpandRunBase extends BasicObject{
    public static final String HELP_INFO = "帮助信息";
    protected Log log = LogFactory.getLog(getClass());
    /**
    * kettleUtil控件
    */
    protected EasyExpand ku;
    protected EasyExpandMeta meta;
    protected EasyExpandData data;
    /**
     * 输入数据行
     */
    protected Object[] inputRow;
    /**
    * 配置信息
    */
    protected JSONObject configInfo;
    /**
     * Creates a new instance of KettleUtilRunBase.
     */
    public EasyExpandRunBase() {
    }

    /**
    * 获取本步骤的输出字段 <br/>
    * @author jingma
    * @param r
    * @param origin
    * @param info
    * @param nextStep
    * @param space
    */
    public abstract void getFields(RowMetaInterface r, String origin, 
            RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) throws Exception;

    /**
    * 开始处理每一行数据 <br/>
    * @author jingma
    * @return 
    * @throws KettleException 
    */
    public boolean run() throws Exception{
        inputRow = ku.getRow(); // get row, blocks when needed!
        if (inputRow == null) // no more input to be expected...
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
        Object[] outputRow = RowDataUtil.createResizedCopy( inputRow, data.outputRowMeta.size() );
        JsonResult result = disposeRow(outputRow);
        //99：不推送数据到下一步
        if(!"99".equals(result.getMsg())){
            //将该记录设置到下一步骤的读取序列中
            ku.putRow(data.outputRowMeta, outputRow);
        }
        return result.isStatus();
    }

    /**
    * 停止操作 <br/>
    * @author jingma
    * @throws Exception
    */
    public void stopAll(){
    }

    /**
    * 处理具体每一行数据 <br/>
    * @author jingma
    * @param outputRow
     * @return 
    */
    protected JsonResult disposeRow(Object[] outputRow) throws Exception{
        return success("默认什么也不做");
    }

    /**
    * 初始化工作 <br/>
    * @author jingma
    */
    protected void init() throws Exception{
    }

    /**
    * 结束工作 <br/>
    * @author jingma
    * @return
    */
    protected void end() throws Exception{
    }
    
    /**
    * 获取格式化后的默认JSON配置参数，供使用者方便快捷的修改配置 <br/>
    * @author jingma
     * @param transMeta 
     * @param stepName 
    */
    public String getDefaultConfigInfo(TransMeta transMeta, String stepName) throws Exception{
        return "{}";
    }

    /**
    * 添加字段 <br/>
    * @author jingma
    * @param r 行
    * @param name 字段名称
    * @param type 类型
    * @param trimType 去除空白规则
    * @param origin 宿主
    * @param comments 描述
     * @return 
    */
    protected ValueMetaInterface addField(RowMetaInterface r, String name, int type,
            int trimType, String origin, String comments) {
        return addField(r, name, type, trimType, origin,comments, 0);
    }
    
    /**
    * 添加字段 <br/>
    * @author jingma
    * @param r 行
    * @param name 字段名称
    * @param type 类型
    * @param trimType 去除空白规则
    * @param origin 宿主
    * @param comments 描述
    * @param length 长度
     * @return 
    */
    @SuppressWarnings("deprecation")
    protected ValueMetaInterface addField(RowMetaInterface r, String name, int type,
            int trimType, String origin, String comments, int length) {
        ValueMetaInterface v = new ValueMeta();
        v.setName(name.toUpperCase());
        v.setType(type);
        v.setTrimType(trimType);
        v.setOrigin(origin);
        v.setComments(comments);
        if(length>0){
            v.setLength(length);
        }
        r.addValueMeta(v);
        return v;
    }
    /**
    * 获取输出字段在数组中的下标 <br/>
    * @see org.pentaho.di.core.row.ValueMetaInterface
    * @author jingma
    * @param field 字段名称
    * @return 下标
    */
    public int getFieldIndex(String field){
        return data.outputRowMeta.indexOfValue(field.toUpperCase());
    }
    /**
    * 获取输出字段的类型 <br/>
    * @author jingma
    * @param field 字段名称
    * @return 下标
    */
    public int getFieldType(String field){
        return data.outputRowMeta.getValueMeta(getFieldIndex(field)).getType();
    }
    
    /**
    * 设置变量到根作业 <br/>
    * @author jingma
    * @param variableName 变量名
    * @param variableValue 变量值
    */
    public void setVariableRootJob(String variableName,String variableValue){
        ku.setVariable(variableName, variableValue);
        Trans trans = ku.getTrans();
        while(trans.getParentTrans()!=null){
            trans.setVariable(variableName, variableValue);
            trans = trans.getParentTrans();
        }
        trans.setVariable(variableName, variableValue);
        Job job = trans.getParentJob();
        while(job!=null){
            job.setVariable(variableName, variableValue);
            job = job.getParentJob();
        }
    }
    /**
    * 从根job获取变量 <br/>
    * @author jingma
    * @param variableName 变量名
    * @return 变量值
    */
    public String getVariavle(String variableName){
        return KettleUtils.getProp(ku,variableName);
    }
    /**
     * @return ku 
     */
    public EasyExpand getKu() {
        return ku;
    }

    /**
     * @param ku the ku to set
     */
    public void setKu(EasyExpand ku) {
        this.ku = ku;
    }

    /**
     * @return meta 
     */
    public EasyExpandMeta getMeta() {
        return meta;
    }

    /**
     * @param meta the meta to set
     * @param space 
     */
    public void setMeta(EasyExpandMeta meta, VariableSpace space) {
        this.meta = meta;
        //将配置信息解析成josn对象,支持变量
        try {
            if(StringUtils.isNotBlank(meta.getConfigInfo())){
                setConfigInfo(JSON.parseObject(space.environmentSubstitute(meta.getConfigInfo())));
            }
        } catch (Exception e) {
            log.info("配置信息转换为JSON对象失败："+meta.getConfigInfo(), e);
        }
    }

    /**
     * @return data 
     */
    public EasyExpandData getData() {
        return data;
    }

    /**
     * @param data the data to set
     */
    public void setData(EasyExpandData data) {
        this.data = data;
    }

    /**
     * @return configInfo 
     */
    public JSONObject getConfigInfo() {
        return configInfo;
    }

    /**
     * @param configInfo the configInfo to set
     */
    public void setConfigInfo(JSONObject configInfo) {
        this.configInfo = configInfo;
    }
}
