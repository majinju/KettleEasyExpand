/**
* Project Name:KettleEasyExpand
* Date:2020年5月17日
* Copyright (c) 2020, jingma All Rights Reserved.
*/

package cn.benma666.bdhc;

import cn.benma666.iframe.DictManager;
import cn.benma666.myutils.StringUtil;

import com.alibaba.fastjson.JSONObject;

/**
 * 比对核查-工具类 <br/>
 * 汇集在多处都需要调用的工具
 * date: 2020年5月17日 <br/>
 * @author jingma
 * @version 
 */
public class BdhcUtil {

    /**
    * 这个号码是否需要比对该资源 <br/>
    * 根据资源、项目、任务的配置判断该号码是否需要比对该资源
    * @author jingma
    * @param hcfs 核查方式
    * @param zylb 资源类别
    * @param hm 号码相关信息
    * @return true:需要，false：不需要
    */
    public static boolean isZy(String hcfs,String zylb,JSONObject hm) {
        if(("01".equals(hcfs)&&!hm.getBooleanValue("xm_zl"))
                ||("02".equals(hcfs)&&!hm.getBooleanValue("xm_ql"))){
            //不进行当前方式的比对
            return false;
        }
        //资源等级
        int zydj = Integer.parseInt(DictManager.zdObjByDmByCache("SYS_BDHC_ZY", zylb).getString("dj"));
        //是否比对该资源
        boolean isZy = true;
        //项目级判断
        if(!hm.getBooleanValue("rw_jczy")){
            //不继承项目的资源配置
            isZy = false;
        }else if(((zydj<hm.getIntValue("xm_dj")||zydj==10)&&StringUtil.indexOf(hm.getString("xm_bdzy"), zylb)==-1)
                    ||StringUtil.indexOf(hm.getString("xm_pczy"), zylb)>-1){
            //（不在等级内且没在要求比对的资源中）或（明确排除资源中）
            isZy =  false;
        }
        //任务级判断
        if(StringUtil.indexOf(hm.getString("rw_pczy"), zylb)>-1){
            //任务级明确不比对
            isZy = false;
        }else if(StringUtil.indexOf(hm.getString("rw_bdzy"), zylb)>-1){
            //任务级明确要比对
            isZy = true;
        }
        //如果任务上没有明确具体资源则按项目配置处理
        return isZy;
    }
}
