/**
* Project Name:myservice
* Date:2018年12月16日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.kettle.ljq;

import org.pentaho.di.core.encryption.Encr;

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
public class SjkglLjq extends DefaultLjq{

    /**
    * 
    * @see cn.benma666.sjgl.DefaultLjq#plcl(cn.benma666.domain.SysSjglSjdx, com.alibaba.fastjson.JSONObject)
    */
    @Override
    public JsonResult plcl(SysSjglSjdx sjdx, JSONObject myParams) {
        //处理类型
        String cllx = myParams.getString(LjqInterface.KEY_CLLX);
        switch (cllx) {
        case "mmjm":
            //密码解密
            try {
                return success("密码解密成功:"+Encr.decryptPasswordOptionallyEncrypted( sjdx.get("pw").toString() ));
            } catch (Exception e) {
                log.error("密码解密失败:"+sjdx, e);
                return error("密码解密失败，请查看系统日志分析原因:"+e.getMessage());
            }
        default:
            return super.plcl(sjdx, myParams);
        }
    }
}
