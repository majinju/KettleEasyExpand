
package cn.benma666.kettle.mytuils;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.entries.special.JobEntrySpecial;

import cn.benma666.constants.UtilConst;
import cn.benma666.db.Db;
import cn.benma666.iframe.DictManager;
import cn.benma666.km.job.JobManager;
import cn.benma666.myutils.StringUtil;
import cn.benma666.sjgl.LjqInterface;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.TypeUtils;

/**
* 任务定时 <br/>
* date: 2016年5月24日 <br/>
* @author jingma
* @version 
*/
public class TimingUtil{
	public static int startTypeId = 0;
    static {
        //设置开始控件类型id
        TimingUtil.startTypeId = TypeUtils.castToInt(JobManager.kettledb.queryStr(
                "select jt.id_jobentry_type from r_jobentry_type jt where jt.code='SPECIAL'"));
    }
	
	/**
	* 将定时对象转为简单可理解的文本 <br/>
	* @author jingma
	* @param timing 定时对象
	* @return
	*/
	public static String showText(JSONObject timing){
        //是否重复
        String isRepeat = timing.getString("repeat");
        String initSatrt = timing.getString("initStart");
        //定时类别
        Integer schedulerType = timing.getIntValue("schedulerType");
        Integer hour = timing.getIntValue("hour");
        Integer minutes = timing.getIntValue("minutes");
        String result = "";
        if(JobEntrySpecial.NOSCHEDULING==schedulerType){
            result = "不需要定时";
        }else if(JobEntrySpecial.INTERVAL==schedulerType){
            Integer intervalSeconds = timing.getIntValue("intervalSeconds");
            Integer intervalMinutes = timing.getIntValue("intervalMinutes");
            if(intervalMinutes==0){
                result = "等"+intervalSeconds+"秒";
            }else{
                result = "等"+intervalMinutes+"分"+intervalSeconds+"秒";
            }
        }else if(JobEntrySpecial.DAILY==schedulerType){
            result = "一天的"+hour+"点"+minutes+"分";
        }else if(JobEntrySpecial.WEEKLY==schedulerType){
            Integer weekDay = timing.getIntValue("weekDay");
            String week = DictManager.zdMcByDm(UtilConst.DICT_CATEGORY_WEEK_DAY, weekDay.toString());
            result = week + "的"+hour+"点"+minutes+"分";
        }else if(JobEntrySpecial.MONTHLY==schedulerType){
            Integer dayOfMonth = timing.getIntValue("dayOfMonth");
            result = "一个月的"+dayOfMonth+"日"+hour+"点"+minutes+"分";
        }else if(JobEntrySpecial.CRON==schedulerType){
            String cron = timing.getString("cron");
            result = cron;
        }
        if(UtilConst.WHETHER_TRUE.equals(isRepeat)){
            result+="/重";
        }
        if(UtilConst.WHETHER_TRUE.equals(initSatrt)){
            result+="/初";
        }
	    String msg=result;
        return msg;
	}
	/**
	* 根据作业id得到定时配置的字符串描述 <br/>
	* @author jingma
	* @param jobId
	* @return
	* @throws KettleException
	*/
	public static String showTextByJobid(int jobId) throws KettleException{
	    return showText(getTimingByJobId(jobId));
	}

    /**
    * 通过作业ID获取作业定时信息<br/>
    * @author jingma
    * @param dbCode 所在资源库代码
    * @param jobId 作业ID
    * @return SATRT控件实体
    */
    public static JSONObject getTimingByJobId(int jobId) {
        Integer startId = getStartIdByJobId(jobId);
        if(startId==null){
            return null;
        }
        String sql = "select ja.value_num,ja.value_str,ja.code from r_jobentry_attribute ja "
                + "where ja.id_jobentry=?";
        List<JSONObject> records = JobManager.kettledb.find(sql, startId);
        JSONObject result = new JSONObject();
        for(JSONObject record:records){
            if(StringUtil.isNotBlank(record.getString("value_str"))){
                result.put(record.getString("code"), record.getString("value_str"));
            }else{
                result.put(record.getString("code"), record.getString("value_num"));
            }
        }
        result.put("repeat", StringUtil.whether(result.getString("repeat")));
        result.put("initStart", StringUtil.whether(result.getString("initStart")));
        return result;
    }

    /**
    * 根据作业id获取该作业的开始控件id <br/>
    * @author jingma
    * @param jobId
    * @return
    */
    public static Integer getStartIdByJobId(int jobId) {
        //START控件的类型编号是74，每个JOB只有一个START控件，所有可以唯一确定
        String sql = "select je.id_jobentry from r_jobentry je where "
                + "je.id_job=? and je.id_jobentry_type=?";
        JSONObject startIdObj = JobManager.kettledb.findFirst(sql, jobId,startTypeId);
        if(startIdObj == null){
            return null;
        }
        Integer startId = startIdObj.getIntValue("id_jobentry");
        return startId;
    }
    /**
    * 保存定时到kettle的表中方式 <br/>
    * 直接修改相关表数据，效率高，存在风险<br/>
    * @author jingma
    * @return
    * @throws KettleException
    */
    public static int saveTimingToKettle(JSONObject timing) throws KettleException {
        int jobId = Integer.parseInt(timing.getString(LjqInterface.FIELD_ID));
        Integer startId = getStartIdByJobId(jobId);
        if(startId==null){
            return 0;
        }
        String sql = "update r_jobentry_attribute ja "
                + "set ja.VALUE_NUM=?,ja.VALUE_STR=? "
                + "where ja.id_jobentry="+startId
                + " and ja.code=?";
        Db db = JobManager.kettledb;
        for(String key:new String[]{"repeat","initStart"}){
            if(StringUtil.isNotBlank(timing.getString(key))){
                db.update(sql, 0,UtilConst.WHETHER_TRUE.equals(timing.getString(key))?"Y":"N",
                        key);
            }
        }
        for(String key:new String[]{"schedulerType","intervalSeconds","intervalMinutes",
                "hour","minutes","weekDay","dayOfMonth"}){
            if(StringUtil.isNotBlank(timing.getString(key))){
                db.update(sql, timing.getIntValue(key), null,key);
            }
        }
        if(StringUtil.isNotBlank(timing.getString("cron"))){
            db.update(sql,0, timing.getString("cron"), "cron");
        }
        
        saveTiming(TimingUtil.showText(getTimingByJobId(jobId)), jobId);
        return jobId;
    }
    /**
    * 将定时设置更新到r_job表中 <br/>
    * @author jingma
    * @param timing
    * @param jobId
    */
    public static void saveTiming(String timing, int jobId) {
        JobManager.kettledb.update("update r_job j set j.timing=? where j.id_job=? ", timing, jobId);
    }
}
