/**
* Project Name:myservice
* Date:2018年12月16日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.kettle.ljq;

import cn.benma666.domain.SysSjglSjdx;
import cn.benma666.km.job.JobManager;
import cn.benma666.myutils.JsonResult;
import cn.benma666.sjgl.DefaultLjq;
import cn.benma666.sjgl.LjqInterface;
import cn.benma666.web.SConf;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.TypeUtils;

/**
 * 作业运行日志拦截器 <br/>
 * date: 2018年12月16日 <br/>
 * @author jingma
 * @version 
 */
public class JobYxrzLjq extends DefaultLjq{

    /**
    * 
    * @see cn.benma666.sjgl.DefaultLjq#plcl(cn.benma666.domain.SysSjglSjdx, com.alibaba.fastjson.JSONObject)
    */
    @Override
    public JsonResult plcl(SysSjglSjdx sjdx, JSONObject myParams) {
        if(!sjdx.get("zykdm").equals(SConf.getVal("ddkettle"))){
            //每个应用只能调度一个资源库，多个时需要部署多份，使用同一个sjsj数据库。
            return error("本应于只能调度"+SConf.getVal("ddkettle")+"数据对象的作业");
        }
        //处理类型
        String cllx = myParams.getString(LjqInterface.KEY_CLLX);
        switch (cllx) {
        case "rz":
            //作业日志
            try {
                JSONObject r = JobManager.getLog(sjdx.get(JobManager.ID_JOB).toString(),
                        TypeUtils.castToInt(sjdx.get("startLineNr")));
                return success("获取日志成功",r);
            } catch (Exception e) {
                log.error("获取作业日志失败:"+sjdx, e);
                return error("获取作业日志失败，请查看系统日志分析原因:"+e.getMessage());
            }
        default:
            return super.plcl(sjdx, myParams);
        }
    }
}
