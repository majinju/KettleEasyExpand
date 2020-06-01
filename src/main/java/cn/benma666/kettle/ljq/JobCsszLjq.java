/**
* Project Name:myservice
* Date:2018年12月16日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.kettle.ljq;

import javax.servlet.http.HttpServletRequest;

import cn.benma666.db.Db;
import cn.benma666.domain.SysSjglSjdx;
import cn.benma666.iframe.DictManager;
import cn.benma666.km.job.JobManager;
import cn.benma666.myutils.JsonResult;
import cn.benma666.myutils.StringUtil;
import cn.benma666.sjgl.DefaultLjq;
import cn.benma666.sjgl.LjqInterface;

import com.alibaba.fastjson.JSONObject;

/**
 * 参数设置拦截器 <br/>
 * date: 2018年12月16日 <br/>
 * @author jingma
 * @version 
 */
public class JobCsszLjq extends DefaultLjq{

    /**
    * 
    * @see cn.benma666.sjgl.DefaultLjq#jcxx(cn.benma666.domain.SysSjglSjdx, java.lang.String, javax.servlet.http.HttpServletRequest)
    */
    @Override
    public JsonResult jcxx(SysSjglSjdx dbSjdx, String myparams,
            HttpServletRequest request) {
        dbSjdx.setDxzt(JobManager.kettledb.getName());
        //获取载体类型
        JSONObject dbObj = DictManager.zdObjByDmByCache(LjqInterface.ZD_SYS_COMMON_SJZT, dbSjdx.getDxzt());
        if(dbObj==null){
            return error("该数据对象数据载体无效："+dbSjdx);
        }
        dbSjdx.setDxztlx(dbObj.getString("lx"));
        dbSjdx.setDxgs(dbObj.getString("yhm"));
        JsonResult r = super.jcxx(dbSjdx, myparams, request);
        return r;
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
    * @see cn.benma666.sjgl.DefaultLjq#save(cn.benma666.domain.SysSjglSjdx, com.alibaba.fastjson.JSONObject)
    */
    @Override
    public JsonResult save(SysSjglSjdx sjdx, JSONObject myParams) {
        JSONObject yobj = myParams.getJSONObject(KEY_YOBJ);
        Db tdb = Db.use(sjdx.getDxzt());
        JSONObject obj = tdb.findFirst("select * from v_job_params t where t.id=?", yobj.getString(FIELD_ID));
        if(StringUtil.isBlank(obj.getString("oid"))){
            //新增
            tdb.update("insert into job_params(id_job,ocode,value,simple_spell,full_spell) values(?,?,?,?,?)", 
                    obj.getIntValue("id_job"),obj.getString("ocode"),yobj.getString("value"),
                    StringUtil.getSimpleSpell(obj.getString("oname")),
                    StringUtil.getFullSpell(obj.getString("oname")));
        }else{
            //更新
            tdb.update("update job_params set value=? where id_job=? and ocode=?",
                    yobj.getString("value"), obj.getIntValue("id_job"),obj.getString("ocode"));
        }
        return success("保存成功");
    }
    
}
