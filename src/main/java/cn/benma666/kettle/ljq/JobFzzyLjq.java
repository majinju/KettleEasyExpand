/**
* Project Name:myservice
* Date:2018年12月16日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.kettle.ljq;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.job.JobMeta;

import cn.benma666.domain.SysSjglSjdx;
import cn.benma666.kettle.mytuils.KettleUtils;
import cn.benma666.km.job.JobManager;
import cn.benma666.myutils.JsonResult;
import cn.benma666.sjgl.DefaultLjq;
import cn.benma666.web.SConf;

import com.alibaba.fastjson.JSONObject;

/**
 * 复制作业拦截器 <br/>
 * date: 2018年12月16日 <br/>
 * @author jingma
 * @version 
 */
public class JobFzzyLjq extends DefaultLjq{

    /**
    * 
    * @see cn.benma666.sjgl.DefaultLjq#plcl(cn.benma666.domain.SysSjglSjdx, com.alibaba.fastjson.JSONObject)
    */
    @Override
    public JsonResult plcl(SysSjglSjdx sjdx, JSONObject myParams) {
        return super.plcl(sjdx, myParams);
    }
    /**
    * 
    * @see cn.benma666.sjgl.DefaultLjq#save(cn.benma666.domain.SysSjglSjdx, com.alibaba.fastjson.JSONObject)
    */
    @Override
    public JsonResult save(SysSjglSjdx sjdx, JSONObject myParams) {
        //处理类型
        JSONObject yobj = myParams.getJSONObject(KEY_YOBJ);
        if(!yobj.getString("zykdm").equals(SConf.getVal("ddkettle"))){
            //每个应用只能调度一个资源库，多个时需要部署多份，使用同一个sjsj数据库。
            return error("本应于只能调度"+SConf.getVal("ddkettle")+"数据对象的作业");
        }
        //复制作业
        String[] jobPathArr = yobj.getString("mbzy").replace("\r", "").split("\n");
        List<JSONObject> jobs = JobManager.kettledb.find("select * from r_job where id_job=?",yobj.getString("ids"));
        JSONObject yJobJson = jobs.get(0);
        String successJob = "";
        String failedJob = "";
        try {
            JobMeta yJob = KettleUtils.loadJob(yJobJson.getString("name"), yJobJson.getLongValue("id_directory"));
            for(String jobPath:jobPathArr){
                jobPath = jobPath.replace("\\", "/");
                if(!jobPath.startsWith("/")){
                    jobPath = "/"+jobPath;
                }
                String dir = jobPath.substring(0, jobPath.lastIndexOf("/"));
                String name = jobPath.substring(jobPath.lastIndexOf("/")+1);
                if(StringUtils.isBlank(dir)){
                    dir = "/";
                }
                if(StringUtils.isBlank(name)){
                    failedJob += jobPath+"[作业名称不能为空]\n";
                    continue;
                }
                yJob.setName(name);
                yJob.setRepositoryDirectory(KettleUtils.makeDirs(dir));
                KettleUtils.saveJob(yJob);
                successJob += jobPath+"\n";
            }
        } catch (Exception e) {
            log.error("复制作业失败:"+yobj, e);
            return error("复制作业失败："+e.getMessage()+"\n复制成功的作业：\n"+successJob+"\n复制失败的作业：\n"+failedJob);
        }
        return success("复制成功的作业：\n"+successJob+"\n复制失败的作业：\n"+failedJob);
    }
}
