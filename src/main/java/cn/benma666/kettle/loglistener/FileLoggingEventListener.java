/**
* Project Name:KettleEasyExpand
* Date:2018年5月18日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.kettle.loglistener;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs.FileObject;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogLayout;
import org.pentaho.di.core.logging.KettleLoggingEvent;
import org.pentaho.di.core.logging.KettleLoggingEventListener;
import org.pentaho.di.core.logging.LogMessage;
import org.pentaho.di.core.logging.LoggingObject;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.job.Job;

import cn.benma666.constants.UtilConst;
import cn.benma666.km.job.JobManager;
import cn.benma666.myutils.DateUtil;
import cn.benma666.myutils.StringUtil;

import com.alibaba.fastjson.JSON;

/**
 * 定制的文件日志记录监听器 <br/>
 * date: 2018年5月18日 <br/>
 * @author jingma
 * @version 
 */
public class FileLoggingEventListener implements KettleLoggingEventListener {
    /**
    * 插入作业日志SQL
    */
    public static final String SQL_INSERT_JOB_LOG = "insert into job_log(oid,id_job,job_name,start_date,log_file) values(?,?,?,?,?)";
    /**
     * 更新作业日志SQL
     */
    public static final String SQL_UPDATE_JOB_LOG = "update job_log l set l.end_date=?,l.result=?,l.update_date=? where l.oid=?";
     /**
    * 更新作业状态SQL
    */
    public static final String SQL_UPDATE_JOB_STATUS = "update r_job j set j.run_status=?,j.last_update=? where j.id_job=?";
    /**
    * 启动失败
    */
    public static final String START_FAILED = "StartFailed";
    /**
    * 停止失败
    */
    public static final String STOP_FAILED = "StopFailed";
    /**
    * 日志
    */
    private static Log log = LogFactory.getLog(FileLoggingEventListener.class);
    /**
    * 日志预警匹配的正则
    */
    public static Pattern logPatt;
    /**
    * 日志管道仓库
    */
    private static LoggingRegistry lr = LoggingRegistry.getInstance();
    /**
    * <作业，作业对应的日志文件>
    */
    public static Map<Job,FileLoggingEventListener> jobLogListener = new ConcurrentHashMap<Job, FileLoggingEventListener>();
    /**
    * <作业，作业对应的日志文件>
    */
    public static Map<String,FileLoggingEventListener> jobNameLogListener = new ConcurrentHashMap<String, FileLoggingEventListener>();
    /**
    * <作业，作业对应的日志文件>
    */
    private static Map<Job,File> jobLogFile = new ConcurrentHashMap<Job, File>();
    /**
    * <作业,日志主键>
    */
    public static Map<Job,String> jobLogOidMap = new ConcurrentHashMap<Job,String>();
    /**
    * <作业,开始时间>
    */
    public static Map<Job,String> jobStartDateMap = new ConcurrentHashMap<Job,String>();
    /**
    * 日志对应作业
    */
    private Job job;
    /**
    * 是否关闭
    */
    private boolean closed = false;
    /**
    * 最后更新时间
    */
    private Date lastupdate = new Date();
    
    /**
     * Creates a new instance of FileLoggingEventListener.
     */
    public FileLoggingEventListener() {
    }

    /**
    * Creates a new instance of FileLoggingEventListener.
    * @param logChannelId
    * @param filename
    * @param append
     * @param job 
     * @param logSize 
    * @throws KettleException
    */
    public FileLoggingEventListener(String logChannelId, File file,
            boolean append, Job job) throws KettleException {
        this(logChannelId, file.getAbsolutePath(), append);
        this.job = job;
        jobLogFile.put(job, file);
    }
    
    /**
    * 
    * @see org.pentaho.di.core.logging.FileLoggingEventListener#eventAdded(org.pentaho.di.core.logging.KettleLoggingEvent)
    */
    @Override
    public void eventAdded(KettleLoggingEvent event) {
        // 日志是否写入
        try {
              Object messageObject = event.getMessage();
              if ( !(messageObject instanceof LogMessage) ) {
                  log.error("不是LogMessage对象：" + JSON.toJSONString(event));
                  return;
              }
              LogMessage message = (LogMessage) messageObject;
              String tn = Thread.currentThread().getName();
              int end = tn.indexOf(" - ");
              FileLoggingEventListener ll = null;
              if(end>1){
                  ll = jobNameLogListener.get(tn.substring(0, end));
              }
              if (ll == null) {
                  ll = jobNameLogListener.get(message.getSubject());
              }
              // 日志没有写入文件
              if (ll != null) {
                  myLogDispose(event, ll.getJob(),message);
                  ll.setLastupdate(new Date());
                  if(JobManager.isWriteLogFile()){
                      ll.writeLog(event);
                  }
              }else{
                  myLogDispose(event, null, message);
                  log.debug("丢失的kettle日志：" + JSON.toJSONString(event));
              }
        } catch (Exception e) {
            log.error("作业日志处理失败:" + JSON.toJSONString(event), e);
        }
    }

    /**
    * 我的日志处理 ：日志管道时间更新、异常检测<br/>
    * @author jingma
    * @param event
    * @param job
    * @param message
    * @throws Exception
    */
    public void myLogDispose(KettleLoggingEvent event,Job job, LogMessage message) throws Exception {

        //更新日志对象的注册时间，使最新产生日志的日志对象不被移除。
        LoggingObjectInterface lo = lr.getLoggingObject(message.getLogChannelId());
        updateLoggingObject(lo);
        
        // 获取日志内容
        String joblogStr = message.getMessage();
        
        // 日志关键字预警
        Matcher m = logPatt.matcher(joblogStr);
        //没有匹配上且日志级别不是错误
        if (!m.find()&&!message.getLevel().isError()) {
            return;
        }

        if(message.getArguments()!=null&&message.getArguments().length>0){
            //得到异常具体消息，使短信中得到跟具体的信息
            if(message.getArguments()[0] instanceof Throwable){
                joblogStr = ((Throwable)message.getArguments()[0]).getMessage()+Const.CR+joblogStr;
            }
        }
        
        String msg = getExceptionMsg(joblogStr, m);
        String logLevel = message.getLevel().getLevel()+"";
        String error = StringUtil.whether(message.isError());
        String subject = message.getSubject();
        String logChannel = message.getLogChannelId();
        //基于作业获取
        String logFile = "";
        int idJob = 0;
        String jobName = "未知作业："+Thread.currentThread().getName();
        if (job!=null&&jobLogFile.get(job) != null) {
            logFile = jobLogFile.get(job).getAbsolutePath();
            idJob = Integer.parseInt(job.getObjectId().getId());
            jobName = job.getJobMeta().getName();
        }
        //插入到数据库
        JobManager.kettledb.update(
                "insert into job_warning(id_job,job_name,log_file,msg, log_level, error, "
                + "subject,log_channel) values(?,?,?,?,?,?,?,?)",
                idJob, jobName, logFile, msg,logLevel,error,subject,logChannel);
    }

    /**
    * 更新日志对象 <br/>
    * @author jingma
    * @param lo
    */
    public void updateLoggingObject(LoggingObjectInterface loi) {
        if(loi!=null&&loi instanceof LoggingObject){
            LoggingObject lo = (LoggingObject) loi;
            lo.setRegistrationDate(new Date());
            updateLoggingObject(lo.getParent());
        }
    }

    /**
    * 监测文件大小，进行日志文件更换 <br/>
    * @author jingma
    * @throws KettleException
    */
    public static void checkLogFileSize(Job job) throws KettleException {
        // 日志文件大小判断
        if (jobLogFile.get(job) != null&& jobLogFile.get(job).length() > 
                JobManager.getLogFileSize() * 1024 * 1024) {
            // 每个日志文件记录一条作业日志，用户可以根据时间区间选择要下载的日志。
            synchronized ( jobNameLogListener ) {
                close(job);
                addJobLogFile(job);
            }
        }
    }

    /**
    * 更新作业状态 <br/>
    * @author jingma
    * @param job
    */
    public static void updateJobStatus(Job job) {
        JobManager.kettledb.update(SQL_UPDATE_JOB_STATUS,getJobStatus(job),
                JobManager.kettledb.getCurrentDateStr14(),
                Integer.parseInt(job.getObjectId().getId()));
    }

    /**
    * 关闭作业的日志相关资源 <br/>
    * @author jingma
    * @param job
    * @throws KettleException
    */
    public static void close(Job job) throws KettleException {
        synchronized ( jobNameLogListener ) {
            FileLoggingEventListener l = jobLogListener.get(job);
            l.close();
            jobLogFile.remove(job);
            jobLogListener.remove(job);
            jobNameLogListener.remove(job.getJobMeta().getName());
        }
        updateJoblog(job);
    }

    /**
    * 添加作业日志文件 <br/>
    * @author jingma
    * @param job
    * @throws KettleException
    */
    public static void addJobLogFile(Job job) throws KettleException {
        synchronized ( jobNameLogListener ) {
            // 记录日志记录的主键，用于更新
            jobLogOidMap.put(job, StringUtil.getUUIDUpperStr());
            jobStartDateMap.put(job, DateUtil.getGabDate());
            FileLoggingEventListener fileAppender = new FileLoggingEventListener(
                    job.getLogChannelId(),getNewLogFile(job), true , job);
            jobLogListener.put(job, fileAppender);
            jobNameLogListener.put(job.getJobMeta().getName(), fileAppender);
        }
    }
    /**
    * 获取作业新的日志文件 <br/>
    * @author jingma
    * @param job
    * @return
    */
    public static File getNewLogFile(Job job) {
        File logFile;
        logFile = new File(JobManager.getLogFileRoot()+UtilConst.FXG
                +DateUtil.doFormatDate(new Date(), DateUtil.DATE_FORMATTER8));
        if(!logFile.exists()){
            logFile.mkdirs();
        }
        logFile = new File(logFile.getAbsolutePath()+UtilConst.FXG
                +job.getJobname()+"_"+DateUtil.doFormatDate(new Date(), 
                        "HHmmss")+".txt");
        jobLogFile.put(job, logFile);
        //生成日志文件时就插入日志记录，便于用户在运行中查询下载作业日志，因为作业管理只显示最近时间的实时日志
        JobManager.kettledb.update(SQL_INSERT_JOB_LOG, jobLogOidMap.get(job),
                Integer.parseInt(job.getObjectId().getId()),
                job.getJobMeta().getName(),jobStartDateMap.get(job),
                logFile.getAbsolutePath());
        jobStartDateMap.remove(job);
        return logFile;
    }
    /**
    * 每个日志文件记录一条作业日志，用户可以根据时间区间选择要下载的日志。 <br/>
    * @author jingma
    * @param job 作业
    */
    public static void updateJoblog(Job job) {
        String dqsj = JobManager.kettledb.getCurrentDateStr14();
        JobManager.kettledb.update(SQL_UPDATE_JOB_LOG, 
                dqsj,getJobStatus(job),
                dqsj,jobLogOidMap.get(job));
    }
    /**
    * 获取作业运行状态 <br/>
    * @author jingma
    * @param job
    * @return
    */
    public static String getJobStatus(Job job) {
        String status = job.getStatus();
        if(status.indexOf("errors")>-1){
            status = STOP_FAILED;
        }
        return status;
    }

    /**
    * 获取主要异常消息 <br/>
    * @author jingma
    * @param joblogStr
    * @param m
    * @return
    */
    public static String getExceptionMsg(String joblogStr, Matcher m) {
        if(joblogStr.length()<=3000){
            return joblogStr;
        }
        //没有匹配上
        if(!m.find()){
            return joblogStr.substring(0, 3000);
        }
        if(m.start()<=100){
            return joblogStr.substring(0,3000);
        }else if(joblogStr.length()-m.start()+100<=3000){
            return joblogStr.substring(m.start()-100);
        }else{
            return joblogStr.substring(m.start()-100,m.start()+2900);
        }
    }

    /**
     * @return job 
     */
    public Job getJob() {
        return job;
    }

    /**
     * @param job the job to set
     */
    public void setJob(Job job) {
        this.job = job;
    }
    
    ///////////////////////基本原监听器拷贝过来的//////////////////

    private String filename;
    private FileObject file;

    public FileObject getFile() {
      return file;
    }

    private OutputStream outputStream;
    private KettleLogLayout layout;

    private KettleException exception;
    private String logChannelId;

    /**
     * Log all log lines to the specified file
     *
     * @param filename
     * @param append
     * @throws KettleException
     */
    public FileLoggingEventListener( String filename, boolean append ) throws KettleException {
      this( null, filename, append );
    }

    /**
     * Log only lines belonging to the specified log channel ID or one of it's children (grandchildren) to the specified
     * file.
     *
     * @param logChannelId
     * @param filename
     * @param append
     * @throws KettleException
     */
    public FileLoggingEventListener( String logChannelId, String filename, boolean append ) throws KettleException {
      this.logChannelId = logChannelId;
      this.filename = filename;
      this.layout = new KettleLogLayout( true );
      this.exception = null;

      file = KettleVFS.getFileObject( filename );
      outputStream = null;
      try {
        outputStream = KettleVFS.getOutputStream( file, append );
      } catch ( Exception e ) {
        throw new KettleException(
          "Unable to create a logging event listener to write to file '" + filename + "'", e );
      }
    }

    /**
    *  <br/>
    * @author jingma
    * @param event
     * @param message
    * @return 是否添加了日志
    */
    public boolean myEventAdded( KettleLoggingEvent event, LogMessage message ) {

      try {
          boolean logToFile = false;

          if ( logChannelId == null ) {
            logToFile = true;
          } else {
            // This should be fast enough cause cached.
            List<String> logChannelChildren = lr.getLogChannelChildren( logChannelId );
            // This could be non-optimal, consider keeping the list sorted in the logging registry
            logToFile = Const.indexOfString( message.getLogChannelId(), logChannelChildren ) >= 0;
          }

          if ( logToFile ) {
            return true;
          }
      } catch ( Exception e ) {
        exception = new KettleException( "Unable to write to logging event to file '" + filename + "'", e );
      }
      return false;
    }

    public void writeLog(KettleLoggingEvent event) throws IOException {
        String logText = new Date()+layout.format( event );
        outputStream.write( logText.getBytes() );
        outputStream.write( Const.CR.getBytes() );
    }

    public void close() throws KettleException {
      try {
        if ( outputStream != null ) {
          outputStream.close();
        }
        closed = true;
      } catch ( Exception e ) {
        throw new KettleException( "Unable to close output of file '" + filename + "'", e );
      }
    }

    public KettleException getException() {
      return exception;
    }

    public void setException( KettleException exception ) {
      this.exception = exception;
    }

    public String getFilename() {
      return filename;
    }

    public void setFilename( String filename ) {
      this.filename = filename;
    }

    public OutputStream getOutputStream() {
      return outputStream;
    }

    public void setOutputStream( OutputStream outputStream ) {
      this.outputStream = outputStream;
    }

    /**
     * @return closed 
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * @param closed the closed to set
     */
    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    /**
     * @return lastupdate 
     */
    public Date getLastupdate() {
        return lastupdate;
    }

    /**
     * @param lastupdate the lastupdate to set
     */
    public void setLastupdate(Date lastupdate) {
        this.lastupdate = lastupdate;
    }

}
