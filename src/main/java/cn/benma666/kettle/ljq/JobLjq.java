/**
* Project Name:myservice
* Date:2018年12月16日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.kettle.ljq;

import java.awt.image.BufferedImage;
import java.util.List;

import cn.benma666.db.Db;
import cn.benma666.domain.SysSjglFile;
import cn.benma666.domain.SysSjglSjdx;
import cn.benma666.kettle.loglistener.FileLoggingEventListener;
import cn.benma666.kettle.mytuils.KettleUtils;
import cn.benma666.km.job.JobManager;
import cn.benma666.myutils.FileUtil;
import cn.benma666.myutils.JsonResult;
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
        //作业列表
        List<JSONObject> jobs = tdb.find(getDefaultSql(sjdx, "getJobByIds", myParams).getMsg());
        JSONObject jobJson = jobs.get(0);
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
                String dir = KettleUtils.getDirectory(Integer.parseInt(jobJson.getString("id_directory")));
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
        default:
            return super.plcl(sjdx, myParams);
        }
    }
}
