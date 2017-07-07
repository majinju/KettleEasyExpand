/**
 * Copyright (c) 2013-2016, Jieven. All rights reserved.
 *
 * Licensed under the GPL license: http://www.gnu.org/licenses/gpl.txt
 * To use it on other terms please contact us at 1623736450@qq.com
 */
package cn.benma666.kettle.mytuils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.repository.LongObjectId;

import cn.benma666.constants.UtilConst;
import cn.benma666.db.Db;
import cn.benma666.kettle.common.Dict;
import cn.benma666.kettle.common.KuConst;
import cn.benma666.myutils.StringUtil;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.TypeUtils;

/**
* 任务定时 <br/>
* date: 2016年5月24日 <br/>
* @author jingma
* @version 
*/
public class TimingUtil{

	/**
	* 开始控件参数与timing参数名称对应
	*/
	public static Map<String,String> startTimingMap = new HashMap<String, String>();
	public static int startTypeId = 0;
    static {
        startTimingMap.put("start", "start");
        startTimingMap.put("dummy", "dummy");
        startTimingMap.put("repeat", "is_repeat");
        startTimingMap.put("schedulerType", "scheduler_type");
        startTimingMap.put("intervalSeconds", "interval_seconds");
        startTimingMap.put("intervalSeconds", "interval_seconds");
        startTimingMap.put("intervalMinutes", "interval_minutes");
        startTimingMap.put("hour", "hour");
        startTimingMap.put("minutes", "minutes");
        startTimingMap.put("weekDay", "week_day");
        startTimingMap.put("dayOfMonth", "day_of_month");
        //设置开始控件类型id
        TimingUtil.startTypeId = TypeUtils.castToInt(Db.use(KuConst.DS_KETTLE).queryStr(
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
        String isRepeat = timing.getString("is_repeat");
        //定时类别
        String schedulerType = timing.get("scheduler_type").toString();
        String hour = timing.get("hour").toString();
        String minutes = timing.get("minutes").toString();
        String result = "";
        if(KuConst.SCHEDULER_TYPE_NOT_TIMING.equals(schedulerType)){
            result = "不需要定时";
        }else if(KuConst.SCHEDULER_TYPE_TIME_INTERVAL.equals(schedulerType)){
            String intervalSeconds = timing.get("interval_seconds").toString();
            String intervalMinutes = timing.get("interval_minutes").toString();
            if(intervalMinutes.equals("0")){
                result = "等"+intervalSeconds+"秒执行一次";
            }else{
                result = "等"+intervalMinutes+"分"+intervalSeconds+"秒执行一次";
            }
        }else if(KuConst.SCHEDULER_TYPE_DAY.equals(schedulerType)){
            result = "一天的"+hour+"点"+minutes+"分执行一次";
        }else if(KuConst.SCHEDULER_TYPE_WEEK.equals(schedulerType)){
            String weekDay = timing.get("week_day").toString();
            String week = Dict.dictValue(UtilConst.DICT_CATEGORY_WEEK_DAY, weekDay);
            result = week + "的"+hour+"点"+minutes+"分执行一次";
        }else if(KuConst.SCHEDULER_TYPE_MONTH.equals(schedulerType)){
            String dayOfMonth = timing.get("day_of_month").toString();
            result = "一个月的"+dayOfMonth+"日"+hour+"点"+minutes+"分执行一次";
        }
        if(UtilConst.WHETHER_TRUE.equals(isRepeat)){
            result+="/重复执行";
        }
	    String msg=result;
        return msg;
	}

    /**
    * 转换为cron格式的定时配置 ，不需要重复执行的任务不适应与此表达式<br/>
    * @author jingma
    * @return 不重复执行：null，重复执行：6位的定时表达式
    */
    public static String getCron(JSONObject timing){
        //是否重复
        String isRepeat = timing.getString("is_repeat");
        //不需要重复执行的任务需要额外处理
        if(UtilConst.WHETHER_FALSE.equals(isRepeat)){
            return null;
        }
        //定时类别
        String schedulerType = timing.get("scheduler_type").toString();
        String hour = timing.get("hour").toString();
        String minutes = timing.get("minutes").toString();
        String result = "";
        if(KuConst.SCHEDULER_TYPE_NOT_TIMING.equals(schedulerType)){
            result = "* * * * * *";
        }else if(KuConst.SCHEDULER_TYPE_TIME_INTERVAL.equals(schedulerType)){
            String intervalMinutes = timing.get("interval_minutes").toString();
            String intervalSeconds = timing.get("interval_seconds").toString();
            if(intervalMinutes.equals("0")){
                result = "*/"+intervalSeconds+" * * * * *";
            }else{
                result = "0 */"+intervalMinutes+" * * * *";
            }
        }else if(KuConst.SCHEDULER_TYPE_DAY.equals(schedulerType)){
            result = "0 "+minutes+" "+hour+" * * *";
        }else if(KuConst.SCHEDULER_TYPE_WEEK.equals(schedulerType)){
            String weekDay = timing.get("week_day").toString();
            String week = Dict.dictValue(UtilConst.DICT_CATEGORY_WEEK_DAY, weekDay);
            result = "0 "+minutes+" "+hour+" * * "+week+" ";
        }else if(KuConst.SCHEDULER_TYPE_MONTH.equals(schedulerType)){
            String dayOfMonth = timing.get("day_of_month").toString();
            result = "0 "+minutes+" "+hour+" "+dayOfMonth+" * * ";
        }
        return result;
    }

    /**
    * 通过作业ID获取作业定时信息<br/>
    * @author jingma
    * @param dbCode 所在资源库代码
    * @param jobId 作业ID
    * @return SATRT控件实体
    */
    public static JSONObject getTimingByJobId(int jobId) {
        Integer startId = getStartIdByJobId(KuConst.DS_KETTLE, jobId);
        if(startId==null){
            return null;
        }
        String sql = "select ja.value_num,ja.value_str,ja.code from r_jobentry_attribute ja "
                + "where ja.id_jobentry=?";
        List<JSONObject> records = Db.use(KuConst.DS_KETTLE).find(sql, startId);
        JSONObject result = new JSONObject();
        for(JSONObject record:records){
            if(StringUtil.isNotBlank(record.getString("value_str"))){
                result.put(startTimingMap.get(record.getString("code")), record.getString("value_str"));
            }else{
                result.put(startTimingMap.get(record.getString("code")), record.getInteger("value_num"));
            }
        }
        result.put("is_repeat", StringUtil.whether(result.getString("is_repeat")));
        return result;
    }

    /**
    * 通过作业ID获取对应的START控件 <br/>
    * @author jingma
    * @param dbCode 所在资源库代码
    * @param jobId 作业ID
    * @return SATRT控件实体
     * @throws KettleException 
    */
    public static JobEntrySpecial getStartEntryByJobId(int jobId) throws KettleException {
        return getStartEntryByJobId(KuConst.DS_KETTLE, jobId);
    }
    /**
    * 通过作业ID获取对应的START控件 <br/>
    * @author jingma
    * @param dbCode 所在资源库代码
    * @param jobId 作业ID
    * @return SATRT控件实体
     * @throws KettleException 
    */
    public static JobEntrySpecial getStartEntryByJobId(String dbCode,int jobId) throws KettleException {
        Integer startId = getStartIdByJobId(dbCode, jobId);
        if(startId==null){
            return null;
        }
        LongObjectId id = new LongObjectId(startId);
        return KettleUtils.loadJobEntry(id, new JobEntrySpecial());
    }

    /**
    * 根据作业id获取该作业的开始控件id <br/>
    * @author jingma
    * @param dbCode
    * @param jobId
    * @return
    */
    public static Integer getStartIdByJobId(String dbCode, int jobId) {
        //START控件的类型编号是74，每个JOB只有一个START控件，所有可以唯一确定
        String sql = "select je.id_jobentry from r_jobentry je where "
                + "je.id_job=? and je.id_jobentry_type=?";
        JSONObject startIdObj = Db.use(dbCode).findFirst(sql, jobId,startTypeId);
        if(startIdObj == null){
            return null;
        }
        Integer startId = startIdObj.getInteger("id_jobentry");
        return startId;
    }

    /**
    * 从kettle的start控件中获取定时信息 <br/>
    * @author jingma
    * @param start
    */
    public static JSONObject getFromStart(JobEntrySpecial start) {
        if(start==null){
            return null;
        }
        JSONObject result = new JSONObject();
        result.put("is_repeat", StringUtil.whether(start.isRepeat()));
        result.put("scheduler_type", ""+start.getSchedulerType());
        result.put("interval_minutes", start.getIntervalMinutes());
        result.put("interval_seconds", start.getIntervalSeconds());
        result.put("hour", start.getHour());
        result.put("minutes", start.getMinutes());
        result.put("week_day", start.getWeekDay());
        result.put("day_of_month", start.getDayOfMonth());
        return result;
    }

    /**
    * 将定时信息设置到kettle的start控件中 <br/>
    * @author jingma
    * @param start
    */
    public static void setStart(JobEntrySpecial start,JSONObject timing) {
        start.setRepeat(UtilConst.WHETHER_TRUE.equals(timing.getString("is_repeat")));
        start.setSchedulerType(Integer.parseInt(timing.getString("scheduler_type")));
        start.setIntervalMinutes(timing.getBigDecimal("interval_minutes").intValue());
        start.setIntervalSeconds(timing.getBigDecimal("interval_seconds").intValue());
        start.setHour(timing.getBigDecimal("hour").intValue());
        start.setMinutes(timing.getBigDecimal("minutes").intValue());
        start.setWeekDay(Integer.parseInt(timing.getString("week_day")));
        start.setDayOfMonth(timing.getBigDecimal("day_of_month").intValue());
    }
    /**
    * 保存定时到kettle的表中方式1 <br/>
    * 这时最正规的方式，但效率低<br/>
    * @author jingma
    * @return
    * @throws KettleException
    */
    public static int saveTimingToKettle1(JSONObject timing) throws KettleException {
        int jobId = Integer.parseInt(timing.getString(UtilConst.FIELD_OID));
        JobMeta jobMeta = KettleUtils.loadJob(jobId);
        JobEntrySpecial start = KettleUtils.findStart(jobMeta);
        setStart(start,timing);
        KettleUtils.saveJob(jobMeta);
        return jobId;
    }
    /**
    * 保存定时到kettle的表中方式2 <br/>
    * 直接修改相关表数据，效率高，存在风险<br/>
    * @author jingma
    * @return
    * @throws KettleException
    */
    public static int saveTimingToKettle2(JSONObject timing) throws KettleException {
        int jobId = Integer.parseInt(timing.getString(UtilConst.FIELD_OID));
        Integer startId = getStartIdByJobId(KuConst.DS_KETTLE, jobId);
        if(startId==null){
            return 0;
        }
        String sql = "update r_jobentry_attribute ja "
                + "set ja.VALUE_NUM=?,ja.VALUE_STR=? "
                + "where ja.id_jobentry="+startId
                + " and ja.code=?";
        Db db = Db.use(KuConst.DS_KETTLE);
        db.update(sql, 0,UtilConst.WHETHER_TRUE.equals(timing.getString("is_repeat"))?"Y":"N",
                "repeat");
        db.update(sql, Integer.parseInt(timing.getString("scheduler_type")),
                null,"schedulerType");
        db.update(sql, timing.getBigDecimal("interval_seconds").intValue(),
                null,"intervalSeconds");
        db.update(sql, timing.getBigDecimal("interval_minutes").intValue(),
                null,"intervalMinutes");
        db.update(sql, timing.getBigDecimal("hour").intValue(),null,"hour");
        db.update(sql, timing.getBigDecimal("minutes").intValue(),null,"minutes");
        db.update(sql, Integer.parseInt(timing.getString("week_day")),null,"weekDay");
        db.update(sql, timing.getBigDecimal("day_of_month").intValue(),
                null,"dayOfMonth");
        return jobId;
    }

    /**
    * jfinal的参数转成json对象 <br/>
    * @author jingma
    * @param paraMap
    * @return
    */
    public static JSONObject mapToJson(Map<String, String[]> paraMap) {
        JSONObject result = new JSONObject();
        result.put(UtilConst.FIELD_OID, paraMap.get(UtilConst.FIELD_OID)[0]);
        result.put("is_repeat", StringUtil.whether(paraMap.get("is_repeat")==null?UtilConst.WHETHER_FALSE:paraMap.get("is_repeat")[0]));
        result.put("scheduler_type", paraMap.get("scheduler_type")[0]);
        result.put("interval_minutes", paraMap.get("interval_minutes")[0]);
        result.put("interval_seconds", paraMap.get("interval_seconds")[0]);
        result.put("hour", paraMap.get("hour")[0]);
        result.put("minutes", paraMap.get("minutes")[0]);
        result.put("week_day", paraMap.get("week_day")[0]);
        result.put("day_of_month", paraMap.get("day_of_month")[0]);
        return result;
    }
}
