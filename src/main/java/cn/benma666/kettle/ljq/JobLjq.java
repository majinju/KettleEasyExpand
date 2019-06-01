/**
* Project Name:sjgl
* Date:2018年12月16日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.kettle.ljq;

import cn.benma666.domain.SysSjglSjdx;
import cn.benma666.myutils.JsonResult;
import cn.benma666.myutils.StringUtil;
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
        case "fzdx":
            //复制对象
            count = 0;
            for(String id:(String[])params.get(KEY_IDS_ARRAY)){
                //获取对象
                SysSjglSjdx newsjdx = sqlManager.single(SysSjglSjdx.class, id);
                newsjdx.setId(StringUtil.getUUIDUpperStr());
                newsjdx.setDxdm(newsjdx.getDxdm()+"_"+db.getCurrentDateStr14());
                newsjdx.setDxmc(newsjdx.getDxmc()+"-复制品");
                sqlManager.insert(newsjdx);
                //复制字段
                params.put("oldSjdxId", id);
                params.put("newSjdx", newsjdx);
                result = DefaultLjq.getDefaultSql(newsjdx, "fzzd", params);
//                sqlManager.executeUpdate(result.getMsg(), params);
                count++;
            }
            return success("成功复制对象个数："+count);
        default:
            return super.plcl(sjdx, params);
        }
    }
}
