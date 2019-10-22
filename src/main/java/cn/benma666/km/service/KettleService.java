/**
* Project Name:kettle-manager
* Date:2017年6月13日
* Copyright (c) 2017, jingma All Rights Reserved.
*/

package cn.benma666.km.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.eval.JobEntryEval;
import org.pentaho.di.job.entries.shell.JobEntryShell;
import org.pentaho.di.job.entries.sql.JobEntrySQL;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

import cn.benma666.domain.SysQxYhxx;
import cn.benma666.domain.SysSjglSjdx;
import cn.benma666.exception.MyException;
import cn.benma666.iframe.BasicObject;
import cn.benma666.iframe.DictManager;
import cn.benma666.kettle.jobentry.easyexpand.JobEntryEasyExpand;
import cn.benma666.kettle.mytuils.KettleUtils;
import cn.benma666.myutils.DesUtil;
import cn.benma666.myutils.JsonResult;
import cn.benma666.myutils.StringUtil;
import cn.benma666.sjgl.DefaultLjq;
import cn.benma666.sjgl.LjqInterface;
import cn.benma666.web.SConf;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * kettle服务类 <br/>
 * date: 2017年6月13日 <br/>
 * @author jingma
 * @version 
 */
public class KettleService extends BasicObject{
    //模板信息
    public static final String templateRoot = "template/";
    public static final String templateHz = "Template.kjb";

    /**
    * 日志
    */
    public static Log log = LogFactory.getLog(KettleService.class);
    
    /**
    * 编辑作业基本信息 <br/>
    * @author jingma
    * @param sjdx
    * @param myParams
    * @param yobj
    * @return
    */
    private static JsonResult editJobInfo(SysSjglSjdx sjdx, JSONObject myParams,
            JSONObject yobj) throws KettleException, KettleXMLException{
        JobMeta jm = null;
        SysQxYhxx user = (SysQxYhxx) myParams.get(LjqInterface.KEY_USER);
        if(StringUtil.isNotBlank(yobj.getString("id_job"))){
            jm = KettleUtils.loadJob(yobj.getString("id_job"));
        }else{
            //创建作业元对象
            jm = new JobMeta(templateRoot+yobj.getString("zylx")+templateHz, KettleUtils.getInstanceRep());
            jm.setName(yobj.getString("name"));
            //设置目录
            String directory = yobj.getString("id_directory");
            RepositoryDirectoryInterface dir = KettleUtils.makeDirs(directory);
            jm.setRepositoryDirectory(dir);
            jm.setCreatedUser(user.getYhxm());
            jm.setCreatedDate(new Date());
        }
        //设置作业描述
        jm.setDescription(yobj.getString("description"));
        jm.setExtendedDescription(yobj.getString("extended_description"));
        jm.setModifiedUser(user.getYhxm());
        jm.setModifiedDate(new Date());
        return success("修改成功",jm);
    }

    /**
    * 设置作业的相对路径 <br/>
     * @return 
    */
    private static JsonResult getJobInfo(SysSjglSjdx sjdx, JSONObject myParams, 
            JSONObject obj) throws KettleException {
        String dir = KettleUtils.getDirectory(obj.getLong("id_directory"));
        JobMeta jm = KettleUtils.loadJob(obj.getString("name"),obj.getLongValue("id_directory"));
        obj.put("id_directory",dir);
        return success("获取成功",jm);
    }

    public static JsonResult editJobJavascript(SysSjglSjdx sjdx, JSONObject myParams, 
            JSONObject yobj) throws Exception {
        JsonResult r = editJobInfo(sjdx, myParams,yobj);
        if(!r.isStatus()){
            return r;
        }
        JobMeta jm = (JobMeta) r.getData();
        JobEntryEval js = (JobEntryEval) jm.findJobEntry(yobj.getString("zylx")).getEntry();
        js.setScript(yobj.getString("js"));
        KettleUtils.saveJob(jm);
        return success("修改成功",jm);
    }
    public static JsonResult getJobJavascript(SysSjglSjdx sjdx, JSONObject myParams, 
            JSONObject obj) throws KettleException{
        JsonResult r = getJobInfo(sjdx, myParams,obj);
        if(!r.isStatus()){
            return r;
        }
        JobMeta jm = (JobMeta) r.getData();
        JobEntryEval js = (JobEntryEval) jm.findJobEntry(obj.getString("zylx")).getEntry();
        obj.put("js", js.getScript());
        return success("获取成功",obj);
    }
    
    /**
    * 编辑作业 <br/>
    * @author jingma
    * @param paraMap
    * @return
    * @throws KettleException 
    * @throws KettleXMLException 
    */
    public static JsonResult editJobShell(SysSjglSjdx sjdx, JSONObject myParams, 
            JSONObject yobj) throws Exception {
        JsonResult r = editJobInfo(sjdx, myParams,yobj);
        if(!r.isStatus()){
            return r;
        }
        JobMeta jm = (JobMeta) r.getData();
        JobEntryShell shell = (JobEntryShell) jm.findJobEntry(yobj.getString("zylx")).getEntry();
        String workPath = yobj.getString("gzlj");
        if(StringUtil.isBlank(workPath)){
            workPath = "/tmp";
        }
        shell.setWorkDirectory(workPath);
        shell.setScript(yobj.getString("shell"));
        KettleUtils.saveJob(jm);
        return success("修改成功",jm);
    }
    
    /**
    * 作业获取 <br/>
    * @author jingma
    * @param jobJson
    * @return
    * @throws KettleException
    */
    public static JsonResult getJobShell(SysSjglSjdx sjdx, JSONObject myParams, 
            JSONObject obj) throws KettleException{
        JsonResult r = getJobInfo(sjdx, myParams,obj);
        if(!r.isStatus()){
            return r;
        }
        JobMeta jm = (JobMeta) r.getData();
        JobEntryShell shell = (JobEntryShell) jm.findJobEntry(obj.getString("zylx")).getEntry();
        obj.put("gzlj", shell.getWorkDirectory());
        obj.put("shell", shell.getScript());
        return success("获取成功",obj);
    }

    /**
    * 编辑作业 <br/>
    * @author jingma
    * @param paraMap
    * @return
    * @throws KettleException 
    * @throws KettleXMLException 
    */
    public static JsonResult editJobSql(SysSjglSjdx sjdx, JSONObject myParams, 
            JSONObject yobj) throws Exception {
        JsonResult r = editJobInfo(sjdx, myParams,yobj);
        if(!r.isStatus()){
            return r;
        }
        JobMeta jm = (JobMeta) r.getData();
        String dbCode = yobj.getString("sjzt");
        JSONObject dbObj = DictManager.zdObjByDmByCache(LjqInterface.ZD_SYS_COMMON_SJZT, dbCode);
        DatabaseMeta dm = KettleUtils.createDatabaseMeta(dbObj.getString("dm"),dbObj.getString("ljc"),
                dbObj.getString("yhm"),DesUtil.decrypt(dbObj.getString("mm"), SConf.getVal("sjzt.mm.ejmm")),
                false,KettleUtils.getInstanceRep());
        JobEntrySQL sql = (JobEntrySQL) jm.findJobEntry(yobj.getString("zylx")).getEntry();
        sql.setSQL(yobj.getString("sql"));
        sql.setDatabase(dm);
        KettleUtils.saveJob(jm);
        return success("修改成功",jm);
    }
    
    /**
    * 作业获取 <br/>
    * @author jingma
    * @param jobJson
    * @return
    * @throws KettleException
    */
    public static JsonResult getJobSql(SysSjglSjdx sjdx, JSONObject myParams, 
            JSONObject obj) throws KettleException{
        JsonResult r = getJobInfo(sjdx, myParams,obj);
        if(!r.isStatus()){
            return r;
        }
        JobMeta jm = (JobMeta) r.getData();
        JobEntrySQL sql = (JobEntrySQL) jm.findJobEntry(obj.getString("zylx")).getEntry();
        obj.put("sql", sql.getSQL());
        obj.put("sjzt", sql.getDatabase().getName());
        return success("获取成功",obj);
    }

    /**
    * 编辑作业 <br/>
    * @author jingma
    * @param paraMap
    * @return
    * @throws KettleException 
    * @throws KettleXMLException 
    */
    public static JsonResult editJobKm(SysSjglSjdx sjdx, JSONObject myParams, 
            JSONObject yobj) throws Exception {
        JsonResult r = editJobInfo(sjdx, myParams,yobj);
        if(!r.isStatus()){
            return r;
        }
        JobMeta jm = (JobMeta) r.getData();
        JobEntryEasyExpand km = (JobEntryEasyExpand) jm.findJobEntry(yobj.getString("zylx")).getEntry();
        km.setClassName(yobj.getString("kmlm"));
        km.setConfigInfo(yobj.getString("kmpz"));
        KettleUtils.saveJob(jm);
        return success("修改成功",jm);
    }
    
    /**
    * 作业获取 <br/>
    * @author jingma
    * @param jobJson
    * @return
    * @throws KettleException
    */
    public static JsonResult getJobKm(SysSjglSjdx sjdx, JSONObject myParams, 
            JSONObject obj) throws KettleException{
        JsonResult r = getJobInfo(sjdx, myParams,obj);
        if(!r.isStatus()){
            return r;
        }
        JobMeta jm = (JobMeta) r.getData();
        JobEntryEasyExpand km = (JobEntryEasyExpand) jm.findJobEntry(obj.getString("zylx")).getEntry();
        obj.put("kmlm", km.getClassName());
        obj.put("kmpz", km.getConfigInfo());
        return success("获取成功",obj);
    }

    /**
    * 编辑对象流转作业 <br/>
    */
    public static JsonResult editJobDxlz(SysSjglSjdx sjdx, JSONObject myParams,
            JSONObject obj) throws Exception {
        //获取当前用户
        SysQxYhxx user = (SysQxYhxx) myParams.get(LjqInterface.KEY_USER);
        //获取流转对象
        JSONObject lydxParams = (JSONObject) DefaultLjq.getJcxxById(obj.getString("lydx")).getData();
        JSONObject mbdxParams = (JSONObject) DefaultLjq.getJcxxById(obj.getString("mbdx")).getData();
        SysSjglSjdx lydx = (SysSjglSjdx) lydxParams.get(LjqInterface.KEY_SJDX);
        SysSjglSjdx mbdx = (SysSjglSjdx) mbdxParams.get(LjqInterface.KEY_SJDX);
        String lzmb = getLzmb(obj,lydx,mbdx).getMsg();
        //更多配置出来
        JSONObject gdpz = JSONObject.parseObject(obj.getString("extended_description"));
        gdpz.put("lydx", obj.getString("lydx"));
        gdpz.put("mbdx", obj.getString("mbdx"));
        gdpz.put("lzmb", lzmb);
        lydxParams.put("gdpz", gdpz);
        //加载模板
        JobMeta jm = null;
        TransMeta zlzh = null;
        //作业参数
        Map<String, String> params = new HashMap<String, String>();
        //流转模板由代码转为路径
        lzmb = "/template/"+DictManager.zdMcByDm("KETTLE_DXLZ_LZMB", lzmb);
        String directory = obj.getString("id_directory")+"/"+obj.getString("name");
        if(StringUtil.isNotBlank(obj.getString("id_job"))){
            jm = KettleUtils.loadJob(obj.getString("id_job"));
            directory = obj.getString("id_directory");
            zlzh = KettleUtils.loadTrans("处理转换",directory);
        }else{
            //创建作业元对象
            jm = KettleUtils.loadJobTP("模板作业",lzmb);
            zlzh = KettleUtils.loadTransTP("处理转换", lzmb);
            jm.setName(obj.getString("name"));
            jm.setCreatedUser(user.getYhxm());
            jm.setCreatedDate(new Date());
            jm.setJobstatus(2);
            
        }
        //加载步骤
        StepMeta scStep = zlzh.findStep("输出");
        StepMeta srStep = zlzh.findStep("输入");
        //字段映射
        JSONArray zdys = gdpz.getJSONArray("字段映射");
        List<String> updateLookup = new ArrayList<String>();
        List<String> updateStream = new ArrayList<String>();
        List<Boolean> update = new ArrayList<Boolean>();
        for(JSONObject ys:zdys.toArray(new JSONObject[]{})){
            updateLookup.add(ys.getString("目标字段"));
            updateStream.add(ys.getString("来源字段"));
            update.add(ys.getBoolean("是否更新"));
        }
        //输入
        switch (lydx.getDxztlx()) {
        case JdbcUtils.ORACLE:
        case JdbcUtils.MYSQL:
            bzscSjksr(lydxParams, lydx, params, srStep);
            break;
        case "ftp":
        case "bdwj":
        default:
            throw new MyException("暂不支持该数据载体类型作为来源："+lydx.getDxztlx());
        }
        //输出
        switch (mbdx.getDxztlx()) {
        case JdbcUtils.ORACLE:
        case JdbcUtils.MYSQL:
            bzscSjksc(mbdx, gdpz, params, lzmb, scStep, updateLookup,
                    updateStream, update);
            break;
        case "ftp":
        case "bdwj":
        default:
            throw new MyException("暂不支持该数据载体类型作为目标："+lydx.getDxztlx());
        }
        //获取目录
        RepositoryDirectoryInterface dir = KettleUtils.makeDirs(directory);
        //设置作业
        jm.setRepositoryDirectory(dir);
        jm.setDescription(obj.getString("description"));
        jm.setExtendedDescription(JSON.toJSONString(gdpz, true));
        jm.setModifiedUser(user.getYhxm());
        jm.setModifiedDate(new Date());
        obj.put("extended_description", jm.getExtendedDescription());
        //设置转换
        zlzh.setRepositoryDirectory(dir);
        zlzh.setObjectId(KettleUtils.getTransformationID(zlzh));
        KettleUtils.setParams(jm, (NamedParams) jm.realClone(false), params );
        //保存
        KettleUtils.saveTrans(zlzh);
        KettleUtils.saveJob(jm);
        return success("修改成功",jm);
    }

    /**
    * 步骤生成-数据库输入 <br/>
    * @author jingma
    */
    public static void bzscSjksr(JSONObject lydxParams, SysSjglSjdx lydx,
            Map<String, String> params, StepMeta srStep) throws Exception,
            IOException, KettleException {
        TableInputMeta ti = (TableInputMeta) srStep.getStepMetaInterface();
        //输入的数据库
        String lyDbDm = lydx.getDxzt();
        JSONObject lyDbObj = DictManager.zdObjByDmByCache(LjqInterface.ZD_SYS_COMMON_SJZT, lyDbDm);
        DatabaseMeta lyDm = KettleUtils.createDatabaseMeta(lyDbObj.getString("dm"),lyDbObj.getString("ljc"),
                lyDbObj.getString("yhm"),DesUtil.decrypt(lyDbObj.getString("mm"), SConf.getVal("sjzt.mm.ejmm")),
                false,KettleUtils.getInstanceRep());
        ti.setDatabaseMeta(lyDm);
        params.put("SOURCE_SQL", DefaultLjq.getDefaultSql(lydx, "cqsql", lydxParams).getMsg());
    }

    /**
    * 步骤生成-数据库输出 <br/>
    * @author jingma
    */
    public static void bzscSjksc(SysSjglSjdx mbdx, JSONObject gdpz,
            Map<String, String> params, String lzmb, StepMeta scStep,
            List<String> updateLookup, List<String> updateStream,
            List<Boolean> update) throws Exception, IOException,
            KettleException {
        params.put("TG_SCHEMA", mbdx.getDxgs());
        params.put("TG_TABLE", mbdx.getJtdx());
        //输出的数据库
        String pltjl = gdpz.getString("输出批量提交量");
        String mbDbDm = mbdx.getDxzt();
        JSONObject mbDbObj = DictManager.zdObjByDmByCache(LjqInterface.ZD_SYS_COMMON_SJZT, mbDbDm);
        DatabaseMeta mbDm = KettleUtils.createDatabaseMeta(mbDbObj.getString("dm"),mbDbObj.getString("ljc"),
                mbDbObj.getString("yhm"),DesUtil.decrypt(mbDbObj.getString("mm"), SConf.getVal("sjzt.mm.ejmm")),
                false,KettleUtils.getInstanceRep());
        lzmb = getScms(lzmb);
        switch (lzmb) {
        case "插入更新":
            InsertUpdateMeta iu = (InsertUpdateMeta) scStep.getStepMetaInterface();
            iu.setCommitSize(pltjl);
            iu.setUpdateBypassed(gdpz.getBoolean("输出不执行更新"));
            iu.setDatabaseMeta(mbDm);
            //设置字段映射
            iu.setUpdateLookup(updateLookup.toArray(new String[]{}));
            iu.setUpdateStream(updateStream.toArray(new String[]{}));
            iu.setUpdate(update.toArray(new Boolean[]{}));
            //设置去重条件
            List<String> keyLookup = new ArrayList<String>();
            List<String> keyStream = new ArrayList<String>();
            List<String> keyCondition = new ArrayList<String>();
            JSONObject[] gxtjs = gdpz.getJSONArray("更新条件").toArray(new JSONObject[]{});
            if(gxtjs.length==0){
                throw new MyException("插入更新模式必须设置更新条件");
            }
            for(JSONObject ys:gxtjs){
                keyLookup.add(ys.getString("目标字段"));
                keyStream.add(ys.getString("来源字段"));
                keyCondition.add(ys.getString("运算符"));
            }
            iu.setKeyLookup(keyLookup.toArray(new String[]{}));
            iu.setKeyStream(keyStream.toArray(new String[]{}));
            iu.setKeyStream2(new String[keyStream.size()]);
            iu.setKeyCondition(keyCondition.toArray(new String[]{}));
            break;
        case "表输出":
            TableOutputMeta to = (TableOutputMeta) scStep.getStepMetaInterface();
            to.setCommitSize(pltjl);
            to.setDatabaseMeta(mbDm);
            to.setTruncateTable(gdpz.getBooleanValue("裁剪表"));
            //设置字段映射
            to.setFieldDatabase(updateLookup.toArray(new String[]{}));
            to.setFieldStream(updateStream.toArray(new String[]{}));
            break;
        default:
            throw new MyException("不支持的输出模式："+lzmb);
        }
    }

    /**
    * 获取输出模式 <br/>
    */
    private static String getScms(String lzmb) {
        int start = lzmb.indexOf("-")+1;
        if(lzmb.indexOf("-",start)>-1){
            //后缀为自定义模板
            lzmb = lzmb.substring(start, lzmb.indexOf("-",start));
        }else{
            //标准模板
            lzmb = lzmb.substring(start, lzmb.length());
        }
        return lzmb;
    }

    /**
    * 获取流转模板 <br/>
    */
    private static JsonResult getLzmb(JSONObject obj, SysSjglSjdx lydx,
            SysSjglSjdx mbdx) {
        String lzmb = obj.getString("lzmb");
        if(StringUtil.isNotBlank(lzmb)){
            return success(lzmb);
        }
        switch (lydx.getDxztlx()) {
        case JdbcUtils.ORACLE:
        case JdbcUtils.MYSQL:
            //后面可将此处改为字典
            lzmb = "bsr-";
            break;
        case "ftp":
        case "bdwj":
        default:
            throw new MyException("暂不支持该数据载体类型作为来源："+lydx.getDxztlx());
        }
        switch (mbdx.getDxztlx()) {
        case JdbcUtils.ORACLE:
        case JdbcUtils.MYSQL:
            lzmb += "crgx";
            break;
        case "ftp":
        case "bdwj":
        default:
            throw new MyException("暂不支持该数据载体类型作为目标："+lydx.getDxztlx());
        }
        return success(lzmb);
    }

    /**
    * 获取对象流转默认配置 <br/>
    * @author jingma
    * @param sjdx
    * @param myParams
    * @param obj
    * @return
    */
    public static JsonResult getDxlzMrpz(SysSjglSjdx sjdx, JSONObject myParams,
            JSONObject obj) {
        //获取基本信息
        JSONObject lydxParams = (JSONObject) DefaultLjq.getJcxxById(obj.getString("lydx")).getData();
        JSONObject mbdxParams = (JSONObject) DefaultLjq.getJcxxById(obj.getString("mbdx")).getData();
        SysSjglSjdx lydx = (SysSjglSjdx) lydxParams.get(LjqInterface.KEY_SJDX);
        SysSjglSjdx mbdx = (SysSjglSjdx) mbdxParams.get(LjqInterface.KEY_SJDX);
        String lzmb = DictManager.zdMcByDm("KETTLE_DXLZ_LZMB", getLzmb(obj, lydx, mbdx).getMsg());
        //创建更多配置对象
        JSONObject gdpz = new JSONObject();
        //处理字段映射
        getMrzdys(lydxParams, mbdxParams, gdpz);
        //来源对象处理
        switch (lydx.getDxztlx()) {
        case JdbcUtils.ORACLE:
        case JdbcUtils.MYSQL:
            gdpz.put("来源抽取模式", "增量");
            break;
        case "ftp":
        case "bdwj":
        default:
            return error("暂不支持该数据载体类型作为来源："+lydx.getDxztlx());
        }
        //目标对象处理
        switch (mbdx.getDxztlx()) {
        case JdbcUtils.ORACLE:
        case JdbcUtils.MYSQL:
            gdpz.put("输出批量提交量", 1000);
            lzmb = getScms(lzmb);
            switch (lzmb) {
            case "插入更新":
                gdpz.put("输出不执行更新", false);
                break;
            case "表输出":
                gdpz.put("裁剪表", false);
                break;
            default:
                throw new MyException("不支持的输出模式："+lzmb);
            }
            break;
        case "ftp":
        case "bdwj":
        default:
            return error("暂不支持该数据载体类型作为目标："+lydx.getDxztlx());
        }
        
        JSONObject r = new JSONObject();
        r.put("extended_description", gdpz.toJSONString());
        return success("获取配置成功", r);
    }

    /**
    * 获取默认字段映射 <br/>
    * @author jingma
    * @param lydxParams
    * @param mbdxParams
    * @param gdpz
    */
    @SuppressWarnings("unchecked")
    private static void getMrzdys(JSONObject lydxParams, JSONObject mbdxParams,
            JSONObject gdpz) {
        JSONArray zdys = new JSONArray();
        JSONArray gxtj = new JSONArray();
        Map<String,JSONObject> lyFields = (Map<String, JSONObject>) lydxParams.get(LjqInterface.KEY_FIELDS);
        Map<String,JSONObject> mbFields = (Map<String, JSONObject>) mbdxParams.get(LjqInterface.KEY_FIELDS);
        Map<String,JSONObject> lyFieldsByid = new HashMap<String,JSONObject>();
        Map<String,JSONObject> lyFieldsByBzzd = new HashMap<String,JSONObject>();
        for(JSONObject ly:lyFields.values()){
            lyFieldsByid.put(ly.getString("id"),ly);
            lyFieldsByBzzd.put(ly.getString("bzzd"),ly);
        }
        for(Entry<String, JSONObject> mbe:mbFields.entrySet()){
            JSONObject mbf = mbe.getValue();
            if("99".equals(mbf.getString("zdywlb"))){
                //跳过虚拟字段
                continue;
            }
            //字段代码相同
            JSONObject lyf = lyFields.get(mbe.getKey());
            if(lyf==null){
                //目标字段的标准字段在来源对象中
                lyf = lyFieldsByid.get(mbf.getString("bzzd"));
                if(lyf==null){
                    //目标字段的是来源对象字段的标准字段
                    lyf = lyFieldsByBzzd.get(mbf.getString("id"));
                }
            }
            if(lyf!=null){
                JSONObject f = new JSONObject();
                f.put("来源字段", lyf.getString("zddm"));
                f.put("目标字段", mbf.getString("zddm"));
                f.put("是否更新", mbf.getBoolean("yxbj"));
                zdys.add(f);
                //更新条件判断
                if(mbf.getIntValue("qcbh")>0){
                    //去重编号大于0表示为去重字段
                    //更新条件
                    JSONObject tj = new JSONObject();
                    tj.put("来源字段", lyf.getString("zddm"));
                    tj.put("目标字段", mbf.getString("zddm"));
                    tj.put("运算符", "=");
                    gxtj.add(tj);
                }
            }
            
        }
        gdpz.put("字段映射", zdys);
        gdpz.put("更新条件", gxtj);
    }

    public static JsonResult getJobDxlz(SysSjglSjdx sjdx, JSONObject myParams, 
            JSONObject obj) throws KettleException{
        JsonResult r = getJobInfo(sjdx, myParams,obj);
        if(!r.isStatus()){
            return r;
        }
        JobMeta jm = (JobMeta) r.getData();
        JSONObject gdpz = JSONObject.parseObject(jm.getExtendedDescription());
        obj.put("lydx", gdpz.remove("lydx"));
        obj.put("mbdx", gdpz.remove("mbdx"));
        obj.put("lzmb", gdpz.remove("lzmb"));
        obj.put("extended_description", gdpz.toJSONString());
        return success("获取成功",obj);
    }

    /**
    *  <br/>
    * @author jingma
    */
    public static void impJob() {
    }

}
