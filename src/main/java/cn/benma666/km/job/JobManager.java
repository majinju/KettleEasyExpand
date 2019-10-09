/**
* Project Name:KettleUtil
* Date:2016年6月28日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.km.job;

import java.awt.image.BufferedImage;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingObject;
import org.pentaho.di.core.logging.LoggingObjectType;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.PersistJobDataAfterExecution;

import cn.benma666.constants.UtilConst;
import cn.benma666.db.Db;
import cn.benma666.job.AbsJob;
import cn.benma666.kettle.common.Dict;
import cn.benma666.kettle.common.KuConst;
import cn.benma666.kettle.loglistener.FileLoggingEventListener;
import cn.benma666.kettle.mytuils.KettleUtils;
import cn.benma666.kettle.mytuils.TimingUtil;
import cn.benma666.myutils.JsonUtil;
import cn.benma666.myutils.StringUtil;
import cn.benma666.web.SConf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 作业具体操作管理 <br/>
 * date: 2016年6月28日 <br/>
 * @author jingma
 * @version 
 */
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class JobManager extends AbsJob {
    /**
    * 日志
    */
    private static Log log = LogFactory.getLog(JobManager.class);
    
    /**
    * 本应用操作的作业视图
    */
    public static String jobViewName = "v_job";
    /**
     * 本项目代码
     */
    public static String projectCode = "KM_LOCALHOST_82";
    /**
     * 资源库所在数据库操作对象
     */
    public static Db kettledb;
    /**
    * 日志管道仓库
    */
    private static LoggingRegistry lr = LoggingRegistry.getInstance();
    /**
    * <作业id,作业>
    */
    private static Map<String,Job> jobMap = new ConcurrentHashMap<String,Job>(); 
    /**
    * <作业,JSON作业>
    */
    private static Map<Job,JSONObject> jsonjobMap = new ConcurrentHashMap<Job,JSONObject>();

    static{
        System.out.println(JobManager.class.getClassLoader());
        kettledb = Db.use(KuConst.DS_KETTLE);
        FileLoggingEventListener.kettledb = kettledb;
        FileLoggingEventListener.logPatt = Pattern.compile(getLogWarning());
    }
    private static Date updateFlag = new Date();
    /**
     * Creates a new instance of GenerateDataBill.
     */
    public JobManager() {
    }

    /**
    * 
    * @throws Exception 
     * @see cn.benma666.km.job.AbsJob#process(org.quartz.JobExecutionContext)
    */
    @Override
    protected void process() throws Exception {
        //要重启的作业
        List<JSONObject> restartList = new ArrayList<JSONObject>();
        //遍历运行的作业
        synchronized (jobMap) {
            Iterator<Entry<String, Job>> jobIter = jobMap.entrySet().iterator();
            Date updateFlag1 = updateFlag;
            updateFlag = new Date();
            while(jobIter.hasNext()){
                Job job = jobIter.next().getValue();
                String status = FileLoggingEventListener.getJobStatus(job);
                if ((job.getResult()!=null&&!job.isActive())
                        ||Trans.STRING_STOPPED.equals(job.getStatus())
                        ||Trans.STRING_FINISHED_WITH_ERRORS.equals(job.getStatus())
                        ||State.TERMINATED.equals(job.getState())) {
                    // 运行结束
                    JSONObject jsonjob = removeJob(job);
                    jobIter.remove();
                    //异常停止且如果设定的异常自动重启次数大于已经自动重启的次数则执行自动重启操作
                    if(FileLoggingEventListener.STOP_FAILED.equals(status)&&
                            jsonjob.getIntValue("auto_restart_num")>
                            jsonjob.getIntValue("auto_restart_num_yj")){
                        restartList.add(jsonjob);
                    }
                }else{
                    //更新日志对象的注册时间，使存活作业的日志对象不被移除。
                    String jlci = job.getLogChannelId();
                    LoggingObject lo = (LoggingObject)lr.getLoggingObject(jlci);
                    if(lo!=null){
                        lo.setRegistrationDate(new Date());
//                        List<String> l = lr.getChildrenMap().get(jlci);
//                        if(l!=null){
//                        //有多线程问题
//                            for(String lci:l){
//                                lo = (LoggingObject)lr.getLoggingObject(lci);
//                                if(lo!=null&&lo.getParent()!=null&&jlci.equals(lo.getParent().getLogChannelId())
//                                        &&LoggingObjectType.JOBENTRY.equals(lo.getObjectType())){
//                                    lo.setRegistrationDate(new Date());
//                                }
//                            }
//                        }
                      Map<String,LoggingObject> loMap = new HashMap<String, LoggingObject>();
                      for(String lci:lr.getLogChannelChildren(jlci)){
                          lo = (LoggingObject)lr.getLoggingObject(lci);
                          //不为空且为该作业直接子对象且是作业实体
                          if(lo!=null&&lo.getParent()!=null&&jlci.equals(lo.getParent().getLogChannelId())
                                  &&LoggingObjectType.JOBENTRY.equals(lo.getObjectType())){
                              //对象重复出现
                              if(loMap.containsKey(lo.getObjectId().getId())){
                                  loMap.put(lo.getObjectId().getId(), null);
                              }else{
                                  loMap.put(lo.getObjectId().getId(), lo);
                              }
                          }
                      }
                      for(Entry<String, LoggingObject> ent:loMap.entrySet()){
                          if(ent.getValue()!=null){
                              ent.getValue().setRegistrationDate(new Date());
                          }
                      }
                    }
                    //刷新日志文件
                    FileLoggingEventListener ll = FileLoggingEventListener.jobLogListener.get(job);
                    if(ll.getLastupdate().getTime()>updateFlag1.getTime()){
                        if(isWriteLogFile()){
                            ll.getOutputStream().flush();
                            FileLoggingEventListener.checkLogFileSize(job);
                        }
                        FileLoggingEventListener.updateJobStatus(job);
                    }
                }
            }
        }
        for(JSONObject jsonjob:restartList){
            startJob(jsonjob);
            //已重启次数+1
            jsonjob.put("auto_restart_num_yj",jsonjob.getIntValue("auto_restart_num_yj")+1);
            info("异常停止，自动重启："+jsonjob);
        }
    }

    /**
    * 移除作业 <br/>
    * @author jingma
    * @param jobJson
    */
    public static void resetJob(JSONObject jobJson){
        String jobId = jobJson.getString("id_job");
        synchronized (jobMap) {
            if(jobMap.containsKey(jobId)){
                removeJob(jobMap.get(jobId));
                jobMap.remove(jobId);
            }
        }
    }

    /**
    * 移除作业 <br/>
    * @author jingma
    * @param job
    */
    public static JSONObject removeJob(Job job) {
        try {
            FileLoggingEventListener.close(job);
        } catch (Exception e) {
            log.debug("关闭日志失败："+job.getName(),e);
        }
        JSONObject jsonjob = jsonjobMap.get(job);
        jsonjobMap.remove(job);
        FileLoggingEventListener.updateJobStatus(job);
        return jsonjob;
    }

    /**
    * 启动时初始化，运行之前在运行的作业 <br/>
    * @author jingma
    * @param view 本应用操作的作业视图
    * @param string 
    */
    public static void init(String view, String projectCode){
        setJobViewName(view);
        setProjectCode(projectCode);

        try {
            KettleLogStore.getAppender().addLoggingEventListener( 
                    new FileLoggingEventListener() );
        } catch (Exception e1) {
            log.error("加日志监听器错误", e1);
        }
        
        String sql = "select * from "+getJobViewName()+" j where run_status=? and project_code = ? order by oorder asc,last_update desc";
        List<JSONObject> list = kettledb.find(sql, Trans.STRING_RUNNING,getProjectCode());
        kettledb.update("update r_job j set run_status=? where run_status=? and project_code = ?", 
                Trans.STRING_WAITING, Trans.STRING_RUNNING,getProjectCode());
        for(final JSONObject job:list){
             try {
                 startJob(job);
             } catch (Exception e) {
                 log.error("启动job失败:"+job, e);
             }
        }
    }

    /**
    * 启动作业 <br/>
    * @author jingma
    * @param jobJson 作业id
    * @return
    * @throws Exception
    */
    public static String startJob(JSONObject jobJson) throws Exception {
        String jobId = jobJson.getString("id_job");
        if(jobMap.containsKey(jobId)){
            return jobMap.get(jobId).getStatus();
        }
        Date start = new Date();
//        JobMeta jm = KettleUtils.loadJob(jobJson.getString("name"),jobJson.getLong("id_directory"));
        JobMeta jm = KettleUtils.loadJob(jobJson.getLong("id_job"));
        log.info("加载作业总耗时："+(new Date().getTime()-start.getTime())+","+jobJson);
        Map<String, JSONObject> paramMap = kettledb.
                findMap("ocode","select * from job_params jp where jp.id_job=?", jobId);
        for(JSONObject param:paramMap.values()){
            //设置参数
            jm.setParameterValue(param.getString(UtilConst.FIELD_OCODE),
                    param.getString("value"));
        }
        jm.setLogLevel(LogLevel.getLogLevelForCode(Dict.dictValue("KETTLE_LOG_LEVEL", jobJson.getString("log_level"))));
        Job job = new Job(KettleUtils.getInstanceRep(), jm);
        job.setLogLevel(jm.getLogLevel());
        jsonjobMap.put(job, jobJson);

        TimingUtil.saveTiming(TimingUtil.showTextByJobid(Integer.parseInt(jobId)),Integer.parseInt(jobId));
        
        return startJob(job);
    }
    /**
    * 启动作业 <br/>
    * @author jingma
    * @param job 作业
    * @return
     * @throws KettleException 
    * @throws Exception
    */
    public synchronized static String startJob(Job job) throws KettleException{
        jobMap.put(job.getObjectId().getId(), job);
        FileLoggingEventListener.addJobLogFile(job);
        job.start();
        String status = FileLoggingEventListener.getJobStatus(job);
        log.info("作业启动完成："+job.getJobname());
        return status;
    }
    /**
    * 停止作业 <br/>
    * @author jingma
    * @param idJob 作业id
    * @return
    * @throws Exception
    */
    public static String stopJob(String idJob) throws Exception {
        Job job = jobMap.get(idJob);
        if(job == null){
            return Trans.STRING_STOPPED;
        }
        KettleUtils.jobStopAll(job);
        String status = FileLoggingEventListener.getJobStatus(job);
        log.info("作业停止完成："+job.getJobname());
        return status;
    }

    /**
    * 结束作业 <br/>
    * @author jingma
    * @param idJob 作业id
    * @return
    * @throws Exception
    */
    public static String killJob(String idJob) throws Exception {
        Job job = jobMap.get(idJob);
        if(job == null){
            return Trans.STRING_STOPPED;
        }
        KettleUtils.jobKillAll(job);
        log.info("作业结束完成："+job.getJobname()+",线程状态："+job.getState());
        String status = FileLoggingEventListener.getJobStatus(job);
        return status;
    }

    /**
    * 获取作业运行日志 <br/>
    * @author jingma
    * @param idJob 作业id
    * @return
    * @throws Exception
    */
    public static JSONObject getLog(String idJob,int startLineNr) throws Exception {
        Job job = jobMap.get(idJob);
        if(job == null){
            return JsonUtil.createResult("not_run", "该作业当前未运行。若想查看历史运行日志信息，请到【基础日志】页面查询并下载对应日志文件。");
        }
        int lastLineNr = KettleLogStore.getLastBufferLineNr();
        String msg = KettleLogStore.getAppender().getBuffer(
                job.getLogChannel().getLogChannelId(), false, 
                startLineNr , lastLineNr ).toString();
        if(StringUtil.isBlank(msg)&&startLineNr==0){
            return JsonUtil.createResult("not_log", "这里只能显示最近较短时间的实时运行日志。若想查看历史运行日志信息，请到【基础日志】页面查询并下载对应日志文件。");
        }
        JSONObject r = JsonUtil.createResult("ok", msg);
        r.put("lastLineNr", lastLineNr);
        return r;
    }

    /**
    *  <br/>
    * @author jingma
    * @param idJob
     * @return 
     * @throws Exception 
    */
    public static BufferedImage getJobImg(JSONObject jobJson) throws Exception {
        Job job = jobMap.get(jobJson.getString("id_job"));
        BufferedImage image = null;
        if(job == null){
            image = KettleUtils.generateJobImage(KettleUtils.loadJob(jobJson.getLong("id_job")));
        }else{
            image = KettleUtils.generateJobImage(job.getJobMeta());
        }
        return image;
    }

    /**
    *  <br/>
    * @author jingma
    * @param idJob
     * @return 
     * @throws Exception 
    */
    public static BufferedImage getTransImg(JSONObject trans) throws Exception {
        TransMeta t = KettleUtils.loadTrans(trans.getLongValue("id_transformation"));
        BufferedImage image = KettleUtils.generateTransformationImage(t);
        return image;
    }
    
    public String getDefaultConfigInfo() throws Exception {
        JSONObject params = new JSONObject();
//        params.put(WRITE_LOG_FILE, writeLogFile);
        return JSON.toJSONString(params, true);
    }

    /**
     * @return writeLogFile 
     */
    public static Boolean isWriteLogFile() {
        return Boolean.valueOf(SConf.getVal("kettle.writeLogFile"));
    }

    /**
     * @return logFileRoot 
     */
    public static String getLogFileRoot() {
        return SConf.getVal("kettle.logFileRoot");
    }
    
    /**
     * @return logFileSize 
     */
    public static double getLogFileSize() {
        return Double.parseDouble(SConf.getVal("kettle.logFileSize"));
    }

    
    /**
     * @return logWarning 
     */
    public static String getLogWarning() {
        return SConf.getVal("kettle.logWarning");
    }

    /**
     * @return jobViewName 
     */
    public static String getJobViewName() {
        return jobViewName;
    }

    /**
     * @param jobViewName the jobViewName to set
     */
    public static void setJobViewName(String jobViewName) {
        if(jobViewName!=null){
            JobManager.jobViewName = jobViewName;
        }
    }
    /**
     * @return projectCode 
     */
    public static String getProjectCode() {
        return projectCode;
    }
    /**
     * @param projectCode the projectCode to set
     */
    public static void setProjectCode(String projectCode) {
        if(projectCode!=null){
            JobManager.projectCode = projectCode;
        }
    }
    /**
     * @return jobMap 
     */
    public static Job getJob(int jobId) {
        return jobMap.get(jobId+"");
    }
    
}
