/**
* Project Name:myservice
* Date:2018年12月16日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.kettle.ljq;

import java.awt.image.BufferedImage;
import java.util.List;

import cn.benma666.db.Db;
import cn.benma666.domain.SysSjglFile;
import cn.benma666.domain.SysSjglSjdx;
import cn.benma666.kettle.mytuils.KettleUtils;
import cn.benma666.km.job.JobManager;
import cn.benma666.myutils.FileUtil;
import cn.benma666.myutils.JsonResult;
import cn.benma666.sjgl.DefaultLjq;
import cn.benma666.sjgl.LjqInterface;

import com.alibaba.fastjson.JSONObject;

/**
 * 转换拦截器 <br/>
 * date: 2018年12月16日 <br/>
 * @author jingma
 * @version 
 */
public class TransLjq extends DefaultLjq{

    /**
    * 转换id
    */
    public static final String ID_TRANSFORMATION = "id_transformation";
    /**
    * 
    * @see cn.benma666.sjgl.DefaultLjq#plcl(cn.benma666.domain.SysSjglSjdx, com.alibaba.fastjson.JSONObject)
    */
    @Override
    public JsonResult plcl(SysSjglSjdx sjdx, JSONObject myParams) {
//        if(!sjdx.getDxzt().equals(SConf.getVal("ddkettle"))){
//            //每个应用只能调度一个资源库，多个时需要部署多份，使用同一个sjsj数据库。
//            return error("本应于只能调度"+SConf.getVal("ddkettle")+"数据对象的转换");
//        }
        //处理类型
        String cllx = myParams.getString(LjqInterface.KEY_CLLX);
        //当前数据对象所在数据库
        Db tdb = Db.use(sjdx.getDxzt());
        //失败的转换数
        int flag = 0;
        //转换列表
        List<JSONObject> transList = tdb.find(getDefaultSql(sjdx, "getObjByIds", myParams).getMsg());
        JSONObject transJson = transList.get(0);
        switch (cllx) {
        case KEY_CLLX_PLSC:
            //批量删除转换
            for(JSONObject job : transList){
                try {
                    KettleUtils.delTrans(job.getLongValue(ID_TRANSFORMATION));
                } catch (Exception e) {
                    flag++;
                    log.error("删除job失败:"+job, e);
                }
            }
            if(flag==0){
                return success("删除转换成功："+transList.size());
            }else{
                return error("删除成功转换数："+(transList.size()-flag)+"，失败转换数："+flag+"，请查看系统日志分析原因！");
            }
        case "ml":
            //转换目录
            try {
                String dir = KettleUtils.getDirectory(Integer.parseInt(transJson.getString("id_directory")));
                return success("转换目录："+dir);
            } catch (Exception e) {
                flag++;
                log.error("获取转换目录失败:"+transJson, e);
                return error("获取转换目录失败，请查看系统日志分析原因:"+e.getMessage());
            }
        case "zht":
            //转换图
            try {
                SysSjglFile file = new SysSjglFile();
                file.setWjlx("png");
                file.setWjm(transJson.getString("name")+"的转换图");
                file.setXzms(false);
                BufferedImage image = JobManager.getTransImg(transJson);
                JSONObject r = new JSONObject();
                r.put(KEY_FILE_BYTES, FileUtil.toBytes(image));
                r.put(KEY_FILE_OBJ, file);
                return success("获取转换图成功",r);
            } catch (Exception e) {
                flag++;
                log.error("获取转换图失败:"+transJson, e);
                return error("获取转换图失败，请查看系统日志分析原因:"+e.getMessage());
            }
        case "drzh":
            //导入转换
            return error("暂未实现");
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
        return super.save(sjdx, myParams);
    }
}
