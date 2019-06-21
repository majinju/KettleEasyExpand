/**
* Project Name:sjgl
* Date:2018年12月16日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.kettle.ljq;

import cn.benma666.domain.SysSjglSjdx;
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
        int count = 0;
        JsonResult result = error("未处理");
        switch (cllx) {
        case KEY_CLLX_PLSC:
            //删除相关字段
            result = super.plcl(sjdx, params);
            return result;
        case "qd":
            //启动作业
            //调用远程接口
            return success("成功复制对象个数："+count);
        default:
            return super.plcl(sjdx, params);
        }
    }
}
