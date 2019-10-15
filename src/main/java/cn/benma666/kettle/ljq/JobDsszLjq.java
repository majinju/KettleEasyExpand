/**
* Project Name:myservice
* Date:2018年12月16日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.kettle.ljq;

import javax.servlet.http.HttpServletRequest;

import org.pentaho.di.core.exception.KettleException;
import org.springframework.ui.Model;

import cn.benma666.domain.SysSjglSjdx;
import cn.benma666.kettle.mytuils.TimingUtil;
import cn.benma666.myutils.JsonResult;
import cn.benma666.sjgl.DefaultLjq;
import cn.benma666.web.SConf;

import com.alibaba.fastjson.JSONObject;

/**
 * 作业定时设置拦截器 <br/>
 * date: 2018年12月16日 <br/>
 * @author jingma
 * @version 
 */
public class JobDsszLjq extends DefaultLjq{
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
            sjdx = (SysSjglSjdx) myParams.get(KEY_SJDX);
            JSONObject yobj = myParams.getJSONObject(KEY_YOBJ);
            //取出对应作业的定时信息
            JSONObject timing = TimingUtil.getTimingByJobId(yobj.getInteger(KEY_IDS));
            timing.put(FIELD_ID, yobj.getInteger(KEY_IDS));
            myParams.put(KEY_OBJ, timing);
        }
        return r;
    }
    /**
    * 
    * @see cn.benma666.sjgl.DefaultLjq#save(cn.benma666.domain.SysSjglSjdx, com.alibaba.fastjson.JSONObject)
    */
    @Override
    public JsonResult save(SysSjglSjdx sjdx, JSONObject myParams) {
        JSONObject timing = myParams.getJSONObject(KEY_YOBJ);
        try {
            TimingUtil.saveTimingToKettle(timing);
            return success("保存成功");
        } catch (KettleException e) {
            return error("报错定时信息失败："+e.getMessage(),e);
        }
    }
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
    * @see cn.benma666.sjgl.DefaultLjq#edit(cn.benma666.domain.SysSjglSjdx, org.springframework.ui.Model)
    */
    @Override
    public JsonResult edit(SysSjglSjdx sjdx, JSONObject myParams, Model model) {
        JsonResult r = ok(sjdx,myParams);
        if(!r.isStatus()){
            return r;
        }
        return super.edit(sjdx,myParams, model);
    }
    /**
    * 基础判断过滤 <br/>
    * @author jingma
    * @param sjdx
    * @param myParams
    * @return
    */
    private JsonResult ok(SysSjglSjdx sjdx, JSONObject myParams) {
        if(!sjdx.get("zykdm").equals(SConf.getVal("ddkettle"))){
            //每个应用只能调度一个资源库，多个时需要部署多份，使用同一个sjsj数据库。
            return error("本应于只能调度"+SConf.getVal("ddkettle")+"数据对象的作业");
        }
        return success("ok");
    }
    
}
