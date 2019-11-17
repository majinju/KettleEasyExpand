/**
* Project Name:myservice
* Date:2018年12月16日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.kettle.ljq;

import java.awt.image.BufferedImage;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.pentaho.di.job.JobMeta;
import org.springframework.ui.Model;

import cn.benma666.constants.UtilConst;
import cn.benma666.db.Db;
import cn.benma666.domain.SysSjglFile;
import cn.benma666.domain.SysSjglSjdx;
import cn.benma666.job.JobInterface;
import cn.benma666.kettle.loglistener.FileLoggingEventListener;
import cn.benma666.kettle.mytuils.KettleUtils;
import cn.benma666.km.job.JobManager;
import cn.benma666.km.service.KettleService;
import cn.benma666.myutils.FileUtil;
import cn.benma666.myutils.JsonResult;
import cn.benma666.myutils.StringUtil;
import cn.benma666.sjgl.DefaultLjq;
import cn.benma666.sjgl.LjqInterface;
import cn.benma666.web.SConf;

import com.alibaba.fastjson.JSONObject;

/**
 * 作业拦截器 <br/>
 * date: 2018年12月16日 <br/>
 * @author jingma
 * @version 
 */
public class JobLjq extends DefaultLjq{

    /**
    * 
    * @see cn.benma666.sjgl.DefaultLjq#plcl(cn.benma666.domain.SysSjglSjdx, com.alibaba.fastjson.JSONObject)
    */
    @Override
    public JsonResult plcl(SysSjglSjdx sjdx, JSONObject myParams) {
        if(!sjdx.getDxdm().equals(SConf.getVal("ddkettle"))){
            //每个应用只能调度一个资源库，多个时需要部署多份，使用同一个sjsj数据库。
            return error("本应于只能调度"+SConf.getVal("ddkettle")+"数据对象的作业");
        }
        //处理类型
        String cllx = myParams.getString(LjqInterface.KEY_CLLX);
        //当前数据对象所在数据库
        Db tdb = Db.use(sjdx.getDxzt());
        //运行状态，默认启动失败
        String runStatus = null;
        //失败的作业数
        int flag = 0;
        List<JSONObject> jobs = null;
        JSONObject jobJson = null;
        //作业列表
        if(myParams.containsKey(KEY_IDS_ARRAY)){
            jobs = tdb.find(getDefaultSql(sjdx, "getObjByIds", myParams).getMsg());
            jobJson = jobs.get(0);
        }
        KettleService ks = new KettleService(sjdx,myParams);
        switch (cllx) {
        case KEY_CLLX_PLSC:
            //批量删除作业
            for(JSONObject job : jobs){
                try {
                    KettleUtils.delJob(job.getLongValue(JobManager.ID_JOB));
                } catch (Exception e) {
                    flag++;
                    log.error("删除job失败:"+job, e);
                }
            }
            if(flag==0){
                return success("删除作业成功："+jobs.size());
            }else{
                return error("删除成功作业数："+(jobs.size()-flag)+"，失败作业数："+flag+"，请查看系统日志分析原因！");
            }
        case "qd":
            //启动作业
            for(JSONObject job : jobs){
                runStatus = FileLoggingEventListener.START_FAILED;
                try {
                    runStatus = JobManager.startJob(job);
                } catch (Exception e) {
                    flag++;
                    log.error("启动job失败:"+job, e);
                }
                //更新作业状态
                tdb.update(getDefaultSql(sjdx, "gxzyzt", myParams).getMsg(), runStatus,
                        tdb.getCurrentDateStr14(),job.getString(JobManager.ID_JOB));
            }
            if(flag==0){
                return success("作业启动成功："+jobs.size());
            }else{
                return error("启动成功作业数："+(jobs.size()-flag)+"，失败作业数："+flag+"，请查看系统日志分析原因！");
            }
        case "tz":
            //停止作业
            for(JSONObject job : jobs){
                runStatus = FileLoggingEventListener.STOP_FAILED;
                try {
                    runStatus = JobManager.stopJob(job.getString(JobManager.ID_JOB));
                } catch (Exception e) {
                    flag++;
                    log.error("停止job失败:"+job, e);
                }
                //更新作业状态
                tdb.update(getDefaultSql(sjdx, "gxzyzt", myParams).getMsg(), runStatus,
                        tdb.getCurrentDateStr14(),job.getString(JobManager.ID_JOB));
            }
            if(flag==0){
                return success("作业停止成功："+jobs.size());
            }else{
                return error("停止成功作业数："+(jobs.size()-flag)+"，失败作业数："+flag+"，请查看系统日志分析原因！");
            }
        case "js":
            //结束作业：强制杀死操作
            for(JSONObject job : jobs){
                try {
                    runStatus = JobManager.killJob(job.getString(JobManager.ID_JOB));
                } catch (Exception e) {
                    flag++;
                    log.error("结束job失败:"+job, e);
                }
            }
            if(flag==0){
                return success("作业结束成功："+jobs.size());
            }else{
                return error("结束成功作业数："+(jobs.size()-flag)+"，失败作业数："+flag+"，请查看系统日志分析原因！");
            }
        case "cxsc":
            //重新生成
            myParams.put(KEY_CLLX, KEY_CLLX_UPDATE);
            int qtzy = 0;
            //需要调用每个作业的基础信息
            for(JSONObject job : jobs){
                try {
                    if("dxlz".equals(job.getString("zylx"))){
                        job.put("cxsc", UtilConst.WHETHER_TRUE);
                        ks.setObj(job);
                        ks.getJobDxlz();
                        myParams.put(KEY_OBJ, job);
                        myParams.put(KEY_YOBJ, job);
                        save(sjdx, myParams);
                    }else{
                        qtzy++;
                    }
                } catch (Exception e) {
                    flag++;
                    log.error("重新生成失败:"+job, e);
                }
            }
            if(flag+qtzy==0){
                return success("重新生成成功："+jobs.size());
            }else{
                JsonResult r = error("已重新生成作业数："+(jobs.size()-flag-qtzy));
                if(flag>0){
                    r.addMsg("失败作业数："+flag+"，请查看系统日志分析原因！");
                }else{
                    r.setStatus(true);
                }
                if(qtzy>0){
                    r.addMsg("重新生成功能只针对对象流转类型的作业,其他作业已自动忽略，其他作业数："+qtzy);
                }
                return r;
            }
        case "cz":
            //重置作业，丢弃原有运行信息，用于卡死结束不掉的场景
            try {
                JobManager.resetJob(jobJson);
                return success("作业重置成功");
            } catch (Exception e) {
                flag++;
                log.error("重置job失败:"+jobJson, e);
                return error("作业重置失败，请查看系统日志分析原因:"+e.getMessage());
            }
        case "ml":
            //作业目录
            try {
                String dir = KettleUtils.getDirectory(jobJson.getLong("id_directory"));
                return success("作业目录："+dir);
            } catch (Exception e) {
                flag++;
                log.error("获取作业目录失败:"+jobJson, e);
                return error("获取作业目录失败，请查看系统日志分析原因:"+e.getMessage());
            }
        case "zyt":
            //作业图
            try {
                SysSjglFile file = new SysSjglFile();
                file.setWjlx("png");
                file.setWjm(jobJson.getString("name")+"的作业图");
                file.setXzms(false);
                BufferedImage image = JobManager.getJobImg(jobJson);
                JSONObject r = new JSONObject();
                r.put(KEY_FILE_BYTES, FileUtil.toBytes(image));
                r.put(KEY_FILE_OBJ, file);
                return success("获取作业图成功",r);
            } catch (Exception e) {
                flag++;
                log.error("获取作业图失败:"+jobJson, e);
                return error("获取作业图失败，请查看系统日志分析原因:"+e.getMessage());
            }
        case "drzy":
            //导入作业
            return error("暂未实现");
        case "getKmmrpz":
            //获取KM默认配置
            try {
                JSONObject yobj = myParams.getJSONObject(KEY_YOBJ);
                JSONObject obj = myParams.getJSONObject(KEY_OBJ);
                obj.putAll(yobj);
                String kmlm = obj.getString("kmlm");
                JobInterface ji = (JobInterface) Class.forName(kmlm).newInstance();
                JSONObject km = new JSONObject();
                km.put("kmpz", ji.getDefaultConfigInfo());
                return success("获取成功", km);
            } catch (Exception e) {
                log.error("获取配置失败"+sjdx, e);
                return error("获取配置失败："+e.getMessage());
            }
        case "getDxlzMrpz":
            //获取对象流转默认配置
            try {
                JSONObject yobj = myParams.getJSONObject(KEY_YOBJ);
                JSONObject obj = myParams.getJSONObject(KEY_OBJ);
                obj.putAll(yobj);
                ks.setObj(obj);
                return ks.getDxlzMrpz();
            } catch (Exception e) {
                log.error("获取对象流转默认配置失败"+sjdx, e);
                return error("获取对象流转默认配置失败："+e.getMessage());
            }
        default:
            return super.plcl(sjdx, myParams);
        }
    }
    /**
    * 
    * @see cn.benma666.sjgl.DefaultLjq#jcxx(cn.benma666.domain.SysSjglSjdx, java.lang.String, javax.servlet.http.HttpServletRequest)
    */
    @Override
    public JsonResult jcxx(SysSjglSjdx sjdx, String myparams,
            HttpServletRequest request) {
        JsonResult r = super.jcxx(sjdx, myparams, request);
        if(r.isStatus()){
            JSONObject myParams = (JSONObject) r.getData();
            JSONObject yobj = myParams.getJSONObject(KEY_YOBJ);
            String id_job = yobj.getString("id_job");
            if(StringUtil.isBlank(id_job)){
                //新增
            }else{
                //修改
                JSONObject obj = myParams.getJSONObject(KEY_OBJ);
                try {
                    String zylx = obj.getString("zylx");
                    if(StringUtil.isBlank(zylx)||"cgzy".equals(zylx)){
                        //常规作业
                        return r;
                    }
                    JsonResult r1 = null;
                    KettleService ks = new KettleService(sjdx,myParams);
                    ks.setObj(obj);
                    switch (zylx) {
                    case "javascript":
                        r1  = ks.getJobJavascript();
                        break;
                    case "sql":
                        r1 = ks.getJobSql();
                        break;
                    case "km":
                        r1 = ks.getJobKm();
                        break;
                    case "shell":
                        r1 = ks.getJobShell();
                        break;
                    case "dxlz":
                        r1 = ks.getJobDxlz();
                        break;
                    default:
                        r1 = error("不支持的作业类型："+zylx);
                        break;
                    }
                    if(r1.isStatus()){
                        myParams.put(KEY_OBJ, r1.getData());
                    }else{
                        return r1;
                    }
                } catch (Exception e) {
                    log.error("获取作业信息失败："+yobj,e);
                    r = error("获取作业信息失败："+e.getMessage());
                }
            }
        }
        return r;
    }
    /**
    * 
    * @see cn.benma666.sjgl.DefaultLjq#edit(cn.benma666.domain.SysSjglSjdx, com.alibaba.fastjson.JSONObject, org.springframework.ui.Model)
    */
    @Override
    public JsonResult edit(SysSjglSjdx sjdx, JSONObject myParams, Model model) {
        JsonResult r = super.edit(sjdx, myParams, model);
        return r;
    }
    /**
    * 
    * @see cn.benma666.sjgl.DefaultLjq#save(cn.benma666.domain.SysSjglSjdx, com.alibaba.fastjson.JSONObject)
    */
    @Override
    public JsonResult save(SysSjglSjdx sjdx, JSONObject myParams) {
        String cllx = myParams.getString(KEY_CLLX);
        JSONObject yobj = myParams.getJSONObject(KEY_YOBJ);
        JsonResult r = success("编辑成功");
        try {
            if(KEY_CLLX_UPDATE.equals(cllx)){
                //更新时获取数据库中的记录信息
                JSONObject obj = myParams.getJSONObject(KEY_OBJ);
                //要用前端修改的数据覆盖部分属性，这里克隆一份,最终形成最新的记录信息
                obj = (JSONObject) obj.clone();
                obj.putAll(yobj);
                yobj = obj;
            }
            String zylx = yobj.getString("zylx");
            if(StringUtil.isNotBlank(zylx)&&!"cgzy".equals(zylx)){
                //非常规作业
                KettleService ks = new KettleService(sjdx,myParams);
                ks.setObj(yobj);
                switch (zylx) {
                case "javascript":
                    r = ks.editJobJavascript();
                    break;
                case "sql":
                    r = ks.editJobSql();
                    break;
                case "km":
                    r = ks.editJobKm();
                    break;
                case "shell":
                    r = ks.editJobShell();
                    break;
                case "dxlz":
                    r = ks.editJobDxlz();
                    break;
                default:
                    r = error("不支持的作业类型："+zylx);
                    break;
                }
            }
        } catch (Exception e) {
            log.error("编辑作业失败："+yobj,e);
            r = error("编辑作业失败："+e.getMessage());
        }
        if(r.isStatus()&&KEY_CLLX_INSERT.equals(cllx)){
            //取出作业元数据
            JobMeta jm = (JobMeta) r.getData();
            //设置作业主键到yobj
            String id_job = jm.getObjectId().getId();
            yobj.put(JobManager.ID_JOB, Integer.parseInt(id_job));
            //修改处理类型为更新
            myParams.put(KEY_CLLX,KEY_CLLX_UPDATE);
        }
        if(r.isStatus()){
            //直接数据库更新,更新最后进行，因为kettle修改作业会先删除再添加。
            myParams.put(KEY_YOBJ, yobj);
            r = super.save(sjdx, myParams);
        }
        return r;
    }
}
