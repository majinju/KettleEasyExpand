/**
* Project Name:myservice
* Date:2018年12月16日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.kettle.ljq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cn.benma666.domain.SysSjglSjdx;
import cn.benma666.myutils.JsonResult;
import cn.benma666.sjgl.DefaultLjq;
import cn.benma666.sjgl.LjqInterface;
import cn.benma666.sjgl.LjqManager;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * kettle平台复制拦截器 <br/>
 * 便于部署多套调度系统
 * date: 2018年12月16日 <br/>
 * @author jingma
 * @version 
 */
public class FzptLjq extends DefaultLjq{

    /**
    * 
    * @see cn.benma666.sjgl.DefaultLjq#plcl(cn.benma666.domain.SysSjglSjdx, com.alibaba.fastjson.JSONObject)
    */
    @Override
    public JsonResult plcl(SysSjglSjdx sjdx, JSONObject myParams) {
        //处理类型
        String cllx = myParams.getString(LjqInterface.KEY_CLLX);
        JSONObject yobj = myParams.getJSONObject(KEY_YOBJ);
        switch (cllx) {
        case KEY_CLLX_PLSC:
            //删除
            String[]  fzdxArr = new String[]{"KETTLE_GLPT_ZYGL","KETTLE_GLPT_SJKGL",
                    "KETTLE_GLPT_JCRZ","KETTLE_GLPT_ZYYJ","KETTLE_GLPT_ZYJKPZ","KETTLE_GLPT_ZHGL",
                    "SYS_QX_YHXX_ZYGLDL","SYS_QX_QXXX_ZYGLCD"};
            String gndmhz = yobj.getString("gndmhz");
            String sql = "delete from sys_sjgl_sjdx t where t.dxdm in (";
            for(String dm : fzdxArr){
                sql+="'"+dm+"_"+gndmhz+"',";
            }
            sql = sql.substring(0, sql.length()-1)+")";
            db.update(sql);
            db.update("delete from sys_qx_qxxx t where t.dm like ?","ZYGL_"+gndmhz+"%");
            return success("删除成功");
        default:
            return super.plcl(sjdx, myParams);
        }
    }
    /**
    * 
    * @see cn.benma666.sjgl.DefaultLjq#save(cn.benma666.domain.SysSjglSjdx, com.alibaba.fastjson.JSONObject)
    */
    @Override
    public JsonResult save(SysSjglSjdx sjdx, JSONObject myParams) {
        JSONObject yobj = myParams.getJSONObject(KEY_YOBJ);
        //需要复制的对象
        JSONObject sjdxParams = (JSONObject) DefaultLjq.getJcxxByDxdm("SYS_SJGL_SJDX").getData();
        SysSjglSjdx sjdxDx = (SysSjglSjdx) sjdxParams.get(KEY_SJDX);
        List<JSONObject> list = db.find("select id from sys_sjgl_sjdx t where t.dxdm in ('KETTLE_GLPT_ZYGL',"
                + "'KETTLE_GLPT_SJKGL','KETTLE_GLPT_JCRZ','KETTLE_GLPT_ZYYJ','KETTLE_GLPT_ZYJKPZ',"
                + "'KETTLE_GLPT_ZHGL','SYS_QX_YHXX_ZYGLDL','SYS_QX_QXXX_ZYGLCD')");
        List<String> idList = new ArrayList<String>();
        for(JSONObject o : list){
            idList.add(o.getString("id"));
        }
        sjdxParams.put(KEY_IDS_ARRAY, idList.toArray(new String[idList.size()]));
        sjdxParams.put(KEY_CLLX, "fzdx");
        JSONObject sjdxYobj = new JSONObject();
        sjdxYobj.put("dmhz", yobj.getString("gndmhz"));
        sjdxYobj.put("mchz", yobj.getString("gnmchz"));
        sjdxParams.put(KEY_YOBJ, sjdxYobj);
        LjqManager.plcl(sjdxDx, sjdxParams);
        //KETTLE_GLPT_前缀的作业修改数据载体为新设定的数据源。
        db.update("update sys_sjgl_sjdx t set t.dxzt=? where t.dxdm like ?", 
                yobj.getString("zyksjk"),"KETTLE_GLPT_%"+yobj.getString("gndmhz"));
        //复制ZYGL_前缀的权限，父权限相应修改。
        db.update("insert into sys_qx_qxxx ( px,yxx,kzxx, cjrxm, cjrdm, cjrdwmc, cjrdwdm, mc, dm, ms, lx, ssyy, bz, fqx, dzlx, dkfs, dz, tb)"
                + "select px,yxx,kzxx, cjrxm, cjrdm, cjrdwmc, cjrdwdm, mc, replace(dm,'ZYGL_MR','ZYGL_"+yobj.getString("gndmhz")
                +"'), ms, lx, ssyy, bz, replace(fqx,'ZYGL_MR','ZYGL_"+yobj.getString("gndmhz")
                +"'), dzlx, dkfs, dz, tb from sys_qx_qxxx t where t.dm like 'ZYGL_MR%'");
        //修改系统根节点名称
        db.update("update sys_qx_qxxx t set t.mc=t.mc||'-'||?,t.dz=? where t.dm=?", 
                yobj.getString("gnmchz"),"SYS_QX_QXXX_ZYGLCD_"+yobj.getString("gndmhz"),
                "ZYGL_"+yobj.getString("gndmhz"));
        //修改菜单的数据对象代码
        db.update("update sys_qx_qxxx t set t.dz=t.dz||'_'||? where t.dz like ? and t.dm like ?", 
                yobj.getString("gndmhz"),"KETTLE_GLPT_%","ZYGL_"+yobj.getString("gndmhz")+"%");
        //菜单登陆修改扩展配置
        Map<String, JSONObject> m = db.findMap("dxdm","select dxdm,kzxx from sys_sjgl_sjdx t "
                + "where t.dxdm in ('SYS_QX_YHXX_ZYGLDL','SYS_QX_QXXX_ZYGLCD')");
        //登陆
        JSONObject kzxx = JSON.parseObject(m.get("SYS_QX_YHXX_ZYGLDL").getString("kzxx"));
        JSONObject dlxx = kzxx.getJSONObject("登陆信息");
        dlxx.put("系统名称", dlxx.getString("系统名称")+"-"+yobj.getString("gnmchz"));
        dlxx.put("菜单对象", dlxx.getString("菜单对象")+"_"+yobj.getString("gndmhz"));
        db.update("update sys_sjgl_sjdx t set t.kzxx=? where t.dxdm =?", 
                kzxx.toJSONString(),"SYS_QX_YHXX_ZYGLDL_"+yobj.getString("gndmhz"));
        //菜单
        kzxx = JSON.parseObject(m.get("SYS_QX_QXXX_ZYGLCD").getString("kzxx"));
        JSONObject cdxx = kzxx.getJSONObject("菜单信息");
        cdxx.put("系统名称", cdxx.getString("系统名称")+"-"+yobj.getString("gnmchz"));
        cdxx.put("登陆对象", cdxx.getString("登陆对象")+"_"+yobj.getString("gndmhz"));
        cdxx.put("菜单根节点", "ZYGL_"+yobj.getString("gndmhz"));
        db.update("update sys_sjgl_sjdx t set t.kzxx=? where t.dxdm =?", 
                kzxx.toJSONString(),"SYS_QX_QXXX_ZYGLCD_"+yobj.getString("gndmhz"));
        return success("平台复制成功，进行相关授权即可使用了");
    }
}
