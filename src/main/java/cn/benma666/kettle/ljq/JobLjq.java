/**
* Project Name:myservice
* Date:2018年12月16日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.kettle.ljq;

import java.util.List;

import cn.benma666.db.Db;
import cn.benma666.domain.SysSjglSjdx;
import cn.benma666.kettle.loglistener.FileLoggingEventListener;
import cn.benma666.km.job.JobManager;
import cn.benma666.myutils.JsonResult;
import cn.benma666.sjgl.DefaultLjq;
import cn.benma666.sjgl.LjqInterface;

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
    public JsonResult plcl(SysSjglSjdx sjdx, JSONObject params) {
        String cllx = params.getString(LjqInterface.KEY_CLLX);
        //当前数据对象所在数据库
        Db tdb = Db.use(sjdx.getDxzt());
        //运行状态，默认启动失败
        String runStatus = null;
        //启动失败的作业数
        int flag = 0;
        switch (cllx) {
        case KEY_CLLX_PLSC:
            //批量删除作业
//            result = super.plcl(sjdx, params);
//            return result;
            return error("暂未实现");
        case "qd":
            //启动作业
            List<JSONObject> jobs = tdb.find(getDefaultSql(sjdx, "getJobByIds", params).getMsg());
            for(JSONObject job : jobs){
                runStatus = FileLoggingEventListener.START_FAILED;
                try {
                    runStatus = JobManager.startJob(job);
                } catch (Exception e) {
                    flag++;
                    log.error("启动job失败:"+job, e);
                }
                //更新作业状态
                tdb.update(getDefaultSql(sjdx, "gxzyzt", params).getMsg(), runStatus,
                        tdb.getCurrentDateStr14(),job.getString("id_job"));
            }
            if(flag==0){
                return success("作业启动成功："+jobs.size());
            }else{
                return error("启动成功作业数："+(jobs.size()-flag)+"，失败作业数："+flag+"，请查看系统日志分析原因！");
            }
        case "tz":
            //停止作业
            return error("暂未实现");
        case "js":
            //结束作业：强制杀死操作
            return error("暂未实现");
        case "drzy":
            //导入作业
            return error("暂未实现");
        case "ml":
            //作业目录
            return error("暂未实现");
        case "rz":
            //作业日志
            return error("暂未实现");
        case "fz":
            //复制作业
            return error("暂未实现");
        case "cz":
            //重置作业，丢弃原有运行信息，用于卡死结束不掉的场景
            return error("暂未实现");
        default:
            return super.plcl(sjdx, params);
        }
    }
}
