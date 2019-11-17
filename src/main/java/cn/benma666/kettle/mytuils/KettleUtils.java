package cn.benma666.kettle.mytuils;

import java.awt.image.BufferedImage;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleSecurityException;
import org.pentaho.di.core.gui.AreaOwner;
import org.pentaho.di.core.gui.Point;
import org.pentaho.di.core.gui.SwingGC;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.JobPainter;
import org.pentaho.di.job.entries.job.JobEntryJob;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.BaseRepositoryMeta;
import org.pentaho.di.repository.LongObjectId;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectory;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.filerep.KettleFileRepository;
import org.pentaho.di.repository.filerep.KettleFileRepositoryMeta;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPainter;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.jobexecutor.JobExecutorMeta;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.benma666.constants.UtilConst;
import cn.benma666.domain.SysSjglSjdx;
import cn.benma666.exception.MyException;
import cn.benma666.iframe.CacheFactory;
import cn.benma666.myutils.JdbcUtil;
import cn.benma666.myutils.StringUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

 /**
 * ClassName: KettleUtils <br/>
 * Function: kettle定制化开发工具集. <br/>
 * date: 2015年4月29日 <br/>
 * @author jingma
 * @version 0.0.1
 * @since JDK 1.6
 */
public class KettleUtils {
	/**
	 * LOG:日志
	 */
	public static Logger log = LoggerFactory.getLogger(KettleUtils.class);
    /**
     * repository:kettle资源库
     */
    private static Repository repository;
	/**
	* 资源库Map
	*/
	private static Map<String, Object> repMap = CacheFactory.use("kettlePep", CacheFactory.TYPE_MEMORY);
    /**
    * kettle模板缓存
    */
    private static Map<String, Object> tpMap = CacheFactory.use("kettleTP", CacheFactory.TYPE_MEMORY);

    /**
    * 创建JNDI数据库资源库 <br/>
    * @author jingma
    * @param name 数据库连接名称
    * @param type 数据库类型
    * @param db jndi名称
    * @return
    * @throws KettleException
    */
    public static Repository createDBRepByJndi(String name, String type,
            String db) throws KettleException{
        return createDBRep( name, type, DatabaseMeta.dbAccessTypeCode[DatabaseMeta.TYPE_ACCESS_JNDI], 
                null, db, null, null, null,null);
    }
    
    /**
     * createDBRep:创建数据库资源库. <br/>
     * @author jingma
     * @param name 数据库连接名称
     * @param type 数据库类型
     * @param access 访问类型
     * @param host ip地址
     * @param db 数据库名称
     * @param port 端口
     * @param user 数据库用户名
     * @param pass 数据库密码
     * @param params 参数对象
     * @return 初始化的资源库
     * @throws KettleException 
     * @since JDK 1.6
     */
    public static Repository createDBRep(String name, String type, String access, String host, 
            String db, String port, String user, String pass,JSONObject params) throws KettleException{
        return createDBRep( name, type, access, host, 
             db, port, user, pass, name, name, name+"数据库资源库",params);
    }
    
    /**
    * 创建数据库资源库 <br/>
    * @author jingma
    * @param name 数据库连接名称
    * @param url 数据库连接
    * @param user 数据库用户名
    * @param pass 数据库密码
    * @throws Exception 
    */
    private static Repository createDBRep(String name, String url, String user,String pass) throws Exception {
        return createDBRep( name, url, user, pass, name, name, name+"数据库资源库");
    }

    /**
    * 创建数据库资源库 <br/>
    * @author jingma
    * @param name 数据库连接名称
    * @param url 数据库连接
    * @param user 数据库用户名
    * @param pass 数据库密码
    * @param id 资源库id
    * @param repName 资源库名称
    * @param description 资源库描述
    * @return
     * @throws Exception 
    */
    private static Repository createDBRep(String name, String url, String user,
            String pass, String id, String repName, String description) throws Exception {
        initEnv();
        DatabaseMeta dataMeta = createDatabaseMeta(name,url, user, pass,true,null);
        //资源库元对象
        KettleDatabaseRepositoryMeta kettleDatabaseMeta = 
                new KettleDatabaseRepositoryMeta(id, repName, description, dataMeta);
        return createRep(kettleDatabaseMeta, id, repName, description);
    }
	/**
	 * createDBRep:创建数据库资源库. <br/>
	 * @author jingma
	 * @param name 数据库连接名称
	 * @param type 数据库类型
	 * @param access 访问类型
	 * @param host ip地址
	 * @param db 数据库名称
	 * @param port 端口
	 * @param user 数据库用户名
	 * @param pass 数据库密码
	 * @param id 资源库id
	 * @param repName 资源库名称
	 * @param description 资源库描述
     * @param params 参数对象
	 * @return 已经初始化的资源库
	 * @throws KettleException 
	 * @since JDK 1.6
	 */
	public static Repository createDBRep(String name, String type, String access, String host, 
			String db, String port, String user, String pass,String id, String repName, 
			String description,JSONObject params) throws KettleException{
        initEnv();
		DatabaseMeta dataMeta = createDatabaseMeta(name, type, access, host,
                db, port, user, pass, params,true,null,null);
        //资源库元对象
        KettleDatabaseRepositoryMeta kettleDatabaseMeta = 
                new KettleDatabaseRepositoryMeta(id, repName, description, dataMeta);
        return createRep(kettleDatabaseMeta, id, repName, description);
	}
    /**
     * createFileRep:创建文件资源库. <br/>
     * @author jingma
     * @param id 资源库id
     * @param repName 资源库名称
     * @param description 资源库描述
     * @param baseDirectory 资源库目录
     * @return 已经初始化的资源库
     * @throws KettleException 
     * @since JDK 1.6
     */
    public static Repository createFileRep(String id, String repName, 
            String description, String baseDirectory) throws KettleException{
        initEnv();
        //初始化kettle环境
        if(!KettleEnvironment.isInitialized()){
            KettleEnvironment.init();
        }
        KettleFileRepositoryMeta fileRepMeta = new KettleFileRepositoryMeta( id, repName, description, baseDirectory);
        return createRep(fileRepMeta, id, repName, description);
    }
    /**
    * 创建资源库 <br/>
    * @author jingma
    * @param baseRepositoryMeta 资源库元数据
    * @param id id
    * @param repName 名称
    * @param description 描述
    * @return
    * @throws KettleException
    */
    public static Repository createRep(BaseRepositoryMeta baseRepositoryMeta,
            String id, String repName, String description) throws KettleException{
        if(use(id)!=null){
            if(repository.getName().equals(use(id).getName())){
                repository = null;
            }
            use(id).disconnect();
        }
        Repository repository = null;
        if(baseRepositoryMeta instanceof KettleDatabaseRepositoryMeta){
            //创建资源库对象
            repository = new KettleDatabaseRepository();
            //给资源库赋值
            repository.init((KettleDatabaseRepositoryMeta) baseRepositoryMeta);
        }else{
            //创建资源库对象
            repository = new KettleFileRepository();
            //给资源库赋值
            repository.init((KettleFileRepositoryMeta) baseRepositoryMeta);
        }
        //第一个创建的资源库是默认操作的资源库
        if(KettleUtils.repository==null){
            KettleUtils.repository = repository;
        }
        repMap.put(id, repository);
        log.info(repository.getName()+"资源库初始化成功");
        return repository;
    }
    /**
    * 连接kettle资源库 <br/>
    * @author jingma
     * @param name 数据库连接名称
     * @param type JdbcUtils数据库类型
     * @param kuser kettle数据库用户名
     * @param kpass kettle数据库密码
    * @throws Exception
    */
    public static void connectKettle(String name, String type, String kuser, String kpass) throws Exception {
        KettleUtils.destroy();
        KettleUtils.createDBRepByJndi(name, dbTypeToKettle(type), name);
        KettleUtils.connect(kuser,kpass);
    }
    /**
    * 连接kettle资源库 <br/>
    * @author jingma
     * @param name 数据库连接名称
     * @param url jdbc连接串
     * @param user 数据库用户名
     * @param pass 数据库密码
     * @param kuser kettle数据库用户名
     * @param kpass kettle数据库密码
    * @throws Exception
    */
    public static void connectKettle(String name, String url, String user, String pass, String kuser, String kpass) throws Exception {
        KettleUtils.destroy();
        KettleUtils.createDBRep(name,url, user, pass);
        KettleUtils.connect(kuser,kpass);
    }
    /**
    * 连接kettle资源库 <br/>
    * @author jingma
     * @param name 数据库连接名称
     * @param type JdbcUtils数据库类型
     * @param access 访问类型
     * @param host ip地址
     * @param db 数据库名称
     * @param port 端口
     * @param user 数据库用户名
     * @param pass 数据库密码
     * @param params 参数对象
     * @param kuser kettle数据库用户名
     * @param kpass kettle数据库密码
    * @throws Exception
    */
    public static void connectKettle(String name, String type, String access, String host, 
            String db, String port, String user, String pass, JSONObject params, String kuser, String kpass) throws Exception {
        KettleUtils.destroy();
        KettleUtils.createDBRep(name, dbTypeToKettle(type), access, host, db, port, user, pass,params);
        KettleUtils.connect(kuser,kpass);
    }
	/**
	 * connect:连接资源库. <br/>
	 * @author jingma
	 * @return 连接后的资源库
	 * @throws KettleSecurityException
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static Repository connect() throws KettleSecurityException, KettleException{
		return connect(null,null);
	}
	
	/**
	 * connect:连接资源库. <br/>
	 * @author jingma
	 * @param username 资源库用户名
	 * @param password 资源库密码
	 * @return 连接后的资源库
	 * @throws KettleSecurityException
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static Repository connect(String username,String password) throws KettleSecurityException, KettleException{
		repository.connect(username, password);
		log.info(repository.getName()+"资源库连接成功");
		return repository;
	}
    
    /**
     * getInstance:获取的单例资源库. <br/>
     * @author jingma
     * @return 已经初始化的资源库
     * @throws KettleException 若没有初始化则抛出异常
     * @since JDK 1.6
     */
    public static Repository getInstanceRep() throws KettleException{
        if(repository!=null){
            return repository;
        }else{
            throw new KettleException("没有初始化资源库");
        }
    }
	
    /**
    * 获取指定资源库 <br/>
    * @author jingma
    * @param repId 资源id
    * @return
    */
    public static Repository use(String repId){
        Object rep = repMap.get(repId);
        if(rep==null){
            return null;
        }
        return (Repository) rep;
    }

    /**
    * 根据数据对象获取对应kettle资源库 <br/>
    * @author jingma
    * @param sjdx kettle管理页面的数据对象
    * @return 
    */
    public static Repository use(SysSjglSjdx sjdx){
        Repository rep = use(sjdx.getDxdm());
        if(rep==null){
            //还没初始化
            try {
                //数据对象的kettle配置
                JSONObject kzxx = JSON.parseObject(sjdx.getKzxx()).getJSONObject("kettleConfig");
                Db db = Db.use(sjdx.getDxzt());
                connectKettle(sjdx.getDxzt(),db.getDbType(),kzxx.getString("kuser"), kzxx.getString("kpass"));
                rep = use(sjdx.getDxdm());
            } catch (Exception e) {
                log.error("初始化资源库失败："+sjdx, e);
            }
        }
        if(rep==null){
            //还是没找到就返回默认资源库
            return repository;
        }
        return rep;
    }

    /**
    * destroy:释放资源库. <br/>
    * @author jingma
    * @param repId 释放指定资源库
    */
    public static void destroy(String repId){
        Repository rep = use(repId);
        if(rep!=null){
            rep.disconnect();
            log.info(rep.getName()+"资源库释放成功");
            rep = null;
        }
    }
	/**
	 * destroy:释放资源库. <br/>
	 * @author jingma
	 * @since JDK 1.6
	 */
	public static void destroy(){
		if(repository!=null){
			repository.disconnect();
			log.info(repository.getName()+"资源库释放成功");
            repository = null;
		}
	}
	
    /**
    * 初始化环境变量 <br/>
    * @author jingma
    * @throws KettleException 
    */
    public static void initEnv() throws KettleException {
        //初始化kettle环境
        if(!KettleEnvironment.isInitialized()){
            KettleEnvironment.init();
            KettleClientEnvironment.getInstance().setClient( KettleClientEnvironment.ClientType.SPOON );
        }
    }
	
    /**
    * 数据库类型转换为kettle中的数据库类型 <br/>
    * @author jingma
    * @param dbType
    * @return
    */
    public static String dbTypeToKettle(String dbType) {
        if(UtilConst.DS_TYPE_ORACLE.equals(dbType)){
            return "Oracle";
        }else if(UtilConst.DS_TYPE_MYSQL.equals(dbType)){
            return "mysql";
        }
        return null;
    }
    public static DatabaseMeta createDatabaseMetaByJndi(String jndiName){
        Db db = Db.use(jndiName);
        return createDatabaseMeta(jndiName, db.getDbType(), 
                DatabaseMeta.dbAccessTypeCode[DatabaseMeta.TYPE_ACCESS_JNDI], 
                null, jndiName, null, null,null, null, true, repository, 
                Db.getDbCsyj(db.getDbType(), "select 1 from dual"));
    }
    /**
    * 创建数据连接元数据对象 <br/>
    * @author jingma
     * @param name 数据库连接名称
     * @param url 数据库连接
     * @param user 数据库用户名
     * @param pass 数据库密码
     * @param replace 是否替换已经存在的
     * @param repository 资源库
    * @return
     * @throws Exception 
    */
    public static DatabaseMeta createDatabaseMeta(String name, String url,String user,String pass,
            boolean replace,Repository repository) throws Exception {
        JSONObject urlObj = JdbcUtil.parseJdbcUrl(url);
        if(UtilConst.DS_TYPE_ORACLE.equals(urlObj.getString(JdbcUtil.DB_TYPE))){
            return createDatabaseMeta(name, dbTypeToKettle(urlObj.getString(JdbcUtil.DB_TYPE)), 
                    DatabaseMeta.dbAccessTypeCode[DatabaseMeta.TYPE_ACCESS_NATIVE], 
                    urlObj.getString(JdbcUtil.HOSTNAME), urlObj.getString(JdbcUtil.DATABASE_NAME), 
                    urlObj.getString(JdbcUtil.PORT), user, pass,null,replace,repository,"select 1 from dual");
        }else if(UtilConst.DS_TYPE_MYSQL.equals(urlObj.getString(JdbcUtil.DB_TYPE))){
            return createDatabaseMeta(name, dbTypeToKettle(urlObj.getString(JdbcUtil.DB_TYPE)), 
                    DatabaseMeta.dbAccessTypeCode[DatabaseMeta.TYPE_ACCESS_NATIVE], 
                    urlObj.getString(JdbcUtil.HOSTNAME), urlObj.getString(JdbcUtil.DATABASE_NAME), 
                    urlObj.getString(JdbcUtil.PORT), user, pass,urlObj.getJSONObject(JdbcUtil.PARAM_OBJ),
                    replace,repository,"select 1");
        }else{
            return null;
        }
    }
    /**
    * 创建数据连接元数据对象 <br/>
    * @author jingma
     * @param name 数据库连接名称
     * @param type 数据库类型
     * @param access 访问类型
     * @param host ip地址
     * @param db 数据库名称
     * @param port 端口
     * @param user 数据库用户名
     * @param pass 数据库密码
     * @param params 参数对象
     * @param replace 是否替换已经存在的
     * @param repository 资源库
    * @return
    */
    public static DatabaseMeta createDatabaseMeta(String name, String type,
            String access, String host, String db, String port, String user,
            String pass, JSONObject params,boolean replace,Repository repository,String validationQuery) {
        DatabaseMeta dm = null;
        if(repository!=null){
            //已有资源库
            try {
                ObjectId dbId = repository.getDatabaseID(name);
                if(dbId!=null){
                    //已有该名称的数据源
                    if(!replace){
                        //且不替换则直接返回现有数据源
                        dm = repository.loadDatabaseMeta(dbId, null);
                    }
                }
            } catch (KettleException e) {
                log.error("创建数据库元数据失败", e);
            }
        }
        if(dm==null){
            //创建资源库数据库对象，类似我们在spoon里面创建资源库
            dm = new DatabaseMeta(name, type, access, host, db, port, user, pass);
            if(params!=null){
                for(Entry<String, Object> ent:params.entrySet()){
                    dm.addExtraOption(type, ent.getKey(), ent.getValue()+"");
                }
            }
            //设置强制小写，不然mysql好像有问题
            dm.setForcingIdentifiersToLowerCase(true);
            
            //设置连接池
            if(StringUtil.isNotBlank(validationQuery)){
                dm.setUsingConnectionPool(true);
                dm.setInitialPoolSize(2);
                dm.setMaximumPoolSize(5);
                Properties poolp = new Properties();
//                poolp.put("testOnBorrow", "true");
//                poolp.put("testOnReturn", "true");
                poolp.put("testWhileIdle", "true");
                poolp.put("timeBetweenEvictionRunsMillis", 5*60*1000);
                poolp.put("validationQuery", validationQuery);
                poolp.put("maxActive", 20);
                poolp.put("characterEncoding", "utf-8");
                dm.setConnectionPoolingProperties(poolp );
            }
            
            if(repository!=null){
                //存在资源库则保存到资源库
                try {
                    repository.save(dm, null, null,true);
                } catch (KettleException e) {
                    log.error("保存数据库元数据失败", e);
                }
            }
        }
        return dm;
    }

	/**
	 * loadJob:通过id加载job. <br/>
	 * @author jingma
	 * @param jobId 数字型job的id，数据库资源库时用此方法
	 * @return job元数据
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static JobMeta loadJob(long jobId) throws KettleException {
		return repository.loadJob(new LongObjectId(jobId), null);
	}

	/**
	 * loadJob:通过id加载job. <br/>
	 * @author jingma
	 * @param jobId 字符串job的id，文件资源库时用此方法
	 * @return job元数据
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static JobMeta loadJob(String jobId) throws KettleException {
		return repository.loadJob(new StringObjectId(jobId), null);
	}

	/**
	 * loadTrans:加载作业. <br/>
	 * @author jingma
	 * @param jobname 作业名称
	 * @param directory 作业路径
	 * @return 作业元数据
	 * @throws KettleException 
	 * @since JDK 1.6
	 */
	public static JobMeta loadJob(String jobname, String directory) throws KettleException {
		return loadJob(jobname, directory, repository);
	}
	/**
	 * loadTrans:加载作业. <br/>
	 * @author jingma
	 * @param jobname 作业名称
	 * @param directory 作业路径
	 * @param repository 资源库
	 * @return 作业元数据
	 * @throws KettleException 
	 * @since JDK 1.6
	 */
	public static JobMeta loadJob(String jobname, String directory,Repository repository) throws KettleException {
		RepositoryDirectoryInterface dir = repository.findDirectory(directory);
		return repository.loadJob(jobname,dir,null, null);
	}

    /**
     * loadTrans:加载作业. <br/>
     * @author jingma
     * @param jobname 作业名称
     * @param directory 作业路径
     * @return 作业元数据
     * @throws KettleException 
     * @since JDK 1.6
     */
    public static JobMeta loadJob(String jobname, long directory) throws KettleException {
        return loadJob(jobname, directory, repository);
    }
    /**
     * loadTrans:加载作业. <br/>
     * @author jingma
     * @param jobname 作业名称
     * @param directory 作业路径
     * @param repository 资源库
     * @return 作业元数据
     * @throws KettleException 
     * @since JDK 1.6
     */
    public static JobMeta loadJob(String jobname, long directory,Repository repository) throws KettleException {
        RepositoryDirectoryInterface dir = repository.
                findDirectory(new LongObjectId(directory));
        return repository.loadJob(jobname,dir,null, null);
    }

    /**
    * 删除作业，不级联删除 <br/>
    * @author jingma
    * @param id_job 作业id
    * @throws KettleException
    */
    public static void delJob(long id_job) throws KettleException{
        delJob(id_job,repository);
    }
    /**
    * 删除作业，不级联删除 <br/>
    * @author jingma
    * @param id_job 作业id
    * @param repository 资源库
    * @throws KettleException
    */
    public static void delJob(long id_job,Repository repository) throws KettleException{
        repository.deleteJob(new LongObjectId(id_job));
    }

    /**
    * 删除转换，不级联删除 <br/>
    * @author jingma
    * @param id 作业id
    * @throws KettleException
    */
    public static void delTrans(long id) throws KettleException{
        delTrans(id,repository);
    }
    /**
    * 删除转换，不级联删除 <br/>
    * @author jingma
    * @param id 作业id
    * @param repository 资源库
    * @throws KettleException
    */
    public static void delTrans(long id,Repository repository) throws KettleException{
        repository.deleteTransformation(new LongObjectId(id));
    }

    /**
    * 加载转换 <br/>
    * @author jingma
    * @param id 转换id
    * @return
    * @throws KettleException
    */
    public static TransMeta loadTrans(long id) throws KettleException {
        return repository.loadTransformation(new LongObjectId(id), null);
    }
    
	/**
	 * loadTrans:加载转换. <br/>
	 * @author jingma
	 * @param transname 转换名称
	 * @param directory 转换路径
	 * @return 转换元数据
	 * @throws KettleException 
	 * @since JDK 1.6
	 */
	public static TransMeta loadTrans(String transname, String directory) throws KettleException {
		return loadTrans(transname, directory, repository);
	}
	
	/**
	 * loadTrans:加载转换. <br/>
	 * @author jingma
	 * @param transname 转换名称
	 * @param directory 转换路径
	 * @param repository 资源库
	 * @return 转换元数据
	 * @throws KettleException 
	 * @since JDK 1.6
	 */
	public static TransMeta loadTrans(String transname, String directory,Repository repository) throws KettleException {
		RepositoryDirectoryInterface dir = repository.findDirectory(directory);
		return repository.loadTransformation( transname, dir, null, true, null);
	}

	/**
	 * loadTrans:根据job元数据获取指定转换元数据. <br/>
	 * @author jingma
	 * @param jobMeta job元数据
	 * @param teansName 转换名称
	 * @return 转换元数据
	 * @throws KettleException 
	 * @since JDK 1.6
	 */
	public static TransMeta loadTrans(JobMeta jobMeta, String teansName) throws KettleException {
		JobEntryTrans trans = (JobEntryTrans)(jobMeta.findJobEntry(teansName).getEntry());
		TransMeta transMeta = KettleUtils.loadTrans(trans.getTransname(), trans.getDirectory());
		return transMeta;
	}

	/**
	* 加载作业中的作业实体 <br/>
	* @author jingma
	* @param jobMeta 父作业
	* @param jobEntryName 作业名称
	* @param jobEntryMeta 用于承载将要加载的作业
	* @return
	 * @throws KettleException 
	*/
	public static <T extends JobEntryBase> T loadJobEntry(JobMeta jobMeta, String jobEntryName,
			T jobEntryMeta) throws KettleException {
		return loadJobEntry(jobMeta.findJobEntry(jobEntryName).getEntry().getObjectId(), jobEntryMeta);
	}

    /**
    * 加载作业中的作业实体 <br/>
    * @author jingma
    * @param entryId job实体id
    * @param jobEntryMeta 用于承载将要加载的作业
    * @return
     * @throws KettleException 
    */
    public static <T extends JobEntryBase> T loadJobEntry(ObjectId entryId,
            T jobEntryMeta) throws KettleException {
        jobEntryMeta.loadRep(repository, null, 
                entryId, null,null);
        jobEntryMeta.setObjectId(entryId);
        return jobEntryMeta;
    }

    /**
    * 查找JOB的开始控件 <br/>
    * @author jingma
    * @param jobMeta JOB元数据
    * @return 开始控件
    */
    public static JobEntrySpecial findStart(JobMeta jobMeta) {
        for (int i = 0; i < jobMeta.nrJobEntries(); i++) {
            JobEntryCopy jec = jobMeta.getJobEntry(i);
            JobEntryInterface je = jec.getEntry();
            if (je.getPluginId().equals("SPECIAL")) {
                return (JobEntrySpecial) je;
            }
        }
        return null;
    }
    /**
    * 保存资源库对象 <br/>
    * @author jingma
    * @param repositoryElement 资源库对象
    * @throws KettleException
    */
    public static void saveRepositoryElement(RepositoryElementInterface repositoryElement) throws KettleException {
        saveRepositoryElement(repository,repositoryElement);
    }
    /**
    * 保存资源库对象 <br/>
    * @author jingma
    * @param repository 要保存到的资源库
    * @param repositoryElement 资源库对象
    * @throws KettleException
    */
    public static void saveRepositoryElement(Repository repository,RepositoryElementInterface repositoryElement) throws KettleException {
        repository.save(repositoryElement, null, null, true );
    }

	/**
	 * saveTrans:保存转换. <br/>
	 * @author jingma
	 * @param transMeta 转换元数据
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static void saveTrans(TransMeta transMeta) throws KettleException {
	    saveRepositoryElement(repository,transMeta);
	}

    /**
     * saveTrans:保存转换. <br/>
     * @author jingma
     * @param repository 要保存到的资源库
     * @param transMeta 转换元数据
     * @throws KettleException
     * @since JDK 1.6
     */
    public static void saveTrans(Repository repository,TransMeta transMeta) throws KettleException {
        saveRepositoryElement(repository,transMeta);
    }
    
	/**
	 * saveJob:保存job. <br/>
	 * @author jingma
	 * @param jobMeta job元数据
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static void saveJob(JobMeta jobMeta) throws KettleException {
	    saveRepositoryElement(repository,jobMeta);
	}
    /**
     * saveJob:保存job. <br/>
     * @author jingma
     * @param repository 要保存到的资源库
     * @param jobMeta job元数据
     * @throws KettleException
     * @since JDK 1.6
     */
    public static void saveJob(Repository repository,JobMeta jobMeta) throws KettleException {
        saveRepositoryElement(repository,jobMeta);
    }

	/**
	 * isDirectoryExist:判断指定的job目录是否存在. <br/>
	 * @author jingma
	 * @param repository 
	 * @param directoryName
	 * @return
	 * @since JDK 1.6
	 */
	public static boolean isDirectoryExist(Repository repository, String directoryName) {
		try {
			RepositoryDirectoryInterface dir = repository.findDirectory(directoryName);
			if(dir==null){
				return false;
			}else{
				return true;
			}
		} catch (KettleException e) {
			log.error("判断job目录是否存在失败！",e);
		}
		return false;
	}
    /**
     * 获取或创建目录 <br/>
     * @author jingma
     * @param parentDirectory 父级目录
     * @param directoryName 要创建的目录
     * @return
     * @throws KettleException 
     * @since JDK 1.6
     */
    public static RepositoryDirectoryInterface getOrMakeDirectory(String parentDirectory,String directoryName) throws KettleException {
        RepositoryDirectoryInterface parent = repository.findDirectory(parentDirectory);
        if(StringUtil.isBlank(parentDirectory)){
            parent = new RepositoryDirectory(null, "/");
            parent.setObjectId(new LongObjectId(0));
        }
        if(StringUtil.isNotBlank(directoryName)){
            RepositoryDirectoryInterface dir = repository.findDirectory(parentDirectory+"/"+directoryName);
            if(dir==null){
                dir = repository.createRepositoryDirectory(parent, directoryName);
            }
            return dir;
        }else{
            return parent;
        }
    }
    /**
    * 创建多级目录 <br/>
    * @author jingma
    * @param directoryName
    * @return
    * @throws KettleException
    */
    public static RepositoryDirectoryInterface makeDirs(String directoryName) throws KettleException {
        if(StringUtil.isNotBlank(directoryName)){
            String parentDirectory = "";
            String[] dirArr = directoryName.replace("\\", "/").split("/");
            for(String dirStr:dirArr){
                try {
                    if(StringUtil.isNotBlank(dirStr)){
                        RepositoryDirectoryInterface p = getOrMakeDirectory(parentDirectory, dirStr);
                        parentDirectory = p.getPath();
                    }
                } catch (Exception e) {
                    log.error("创建目录失败："+directoryName+","+parentDirectory+","+dirStr, e);
                }
            }
            return getOrMakeDirectory(parentDirectory,null);
        }else{
            throw new MyException("创建的目录为空");
        }
    }
	
	/**
	 * 获取指定的job目录. <br/>
	 * @author jingma
	 * @param dirId
	 * @return
	 * @throws KettleException 
	 * @since JDK 1.6
	 */
	public static String getDirectory(long dirId) throws KettleException {
	    return getDirectory(new LongObjectId(dirId));
	}
    /**
     * 获取指定的job目录. <br/>
     * @author jingma
     * @param dirId
     * @return
     * @throws KettleException 
     * @since JDK 1.6
     */
    public static String getDirectory(ObjectId dirId) throws KettleException {
        RepositoryDirectoryInterface dir = repository.findDirectory(dirId);
        if(dir==null){
            return null;
        }else{
            return dir.getPath();
        }
    }

	/**
	* 将步骤smi设置到转换trans中<br/>
	* @author jingma
	* @param teans 转换元数据
	* @param stepName 步骤名称
	* @param smi 步骤
	*/
	public static void setStepToTrans(TransMeta teans, String stepName, StepMetaInterface smi) {
		StepMeta step = teans.findStep(stepName);
		step.setStepMetaInterface(smi);
	}

	/**
	* 将步骤smi设置到转换trans中并保存到资源库 <br/>
	* @author jingma
	* @param teans 转换元数据
	* @param stepName 步骤名称
	* @param smi 步骤
	* @throws KettleException 
	*/
	public static void setStepToTransAndSave(TransMeta teans, String stepName, StepMetaInterface smi) throws KettleException {
		setStepToTrans( teans, stepName, smi);
		KettleUtils.saveTrans(teans);
	}

	/**
	* 步骤数据预览 <br/>
	* @author jingma
	* @param teans 转换
	* @param testStep 步骤名称
	* @param smi 步骤实体
	* @param previewSize 预览的条数
	* @return 预览结果
	*/
	public static List<List<Object>> stepPreview(TransMeta teans,
			String testStep, StepMetaInterface smi, int previewSize) {
		TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation(
				teans,
				smi,
				testStep);
		TransPreviewUtil tpu = new TransPreviewUtil(
				previewMeta,
		        new String[] { testStep },
		        new int[] { previewSize } );
		tpu.doPreview();
		return TransPreviewUtil.getData(tpu.getPreviewRowsMeta(testStep),tpu.getPreviewRows(testStep));
	}
	
	/**
	* 停止作业，包含子作业和转换 <br/>
	* @author jingma
	* @param job
	*/
	public static void jobStopAll(Job job){
        job.stopAll();
        JobMeta jobMeta = job.getJobMeta();
        for(JobEntryCopy jec:jobMeta.getJobCopies()){
            if(jec.isTransformation()){
                JobEntryTrans jet = (JobEntryTrans)jec.getEntry();
                if(jet.getTrans()!=null){
                    jet.getTrans().stopAll();
                }
            }else if(jec.isJob()){
                JobEntryJob jej = (JobEntryJob)jec.getEntry();
                if(jej.getJob()!=null){
                    jobStopAll(jej.getJob());
                }
            }
        }
	}
    /**
    * 结束作业，包含子作业和转换 <br/>
    * @author jingma
    * @param job
    */
    @SuppressWarnings("deprecation")
    public static void jobKillAll(Job job){
        job.stopAll();
        JobMeta jobMeta = job.getJobMeta();
        for(JobEntryCopy jec:jobMeta.getJobCopies()){
            if(jec.isTransformation()){
                JobEntryTrans jet = (JobEntryTrans)jec.getEntry();
                if(jet.getTrans()!=null){
                    jet.getTrans().killAll();
                }
            }else if(jec.isJob()){
                JobEntryJob jej = (JobEntryJob)jec.getEntry();
                if(jej.getJob()!=null){
                    jobKillAll(jej.getJob());
                }
            }
        }
        //采用线程中断结束卡住的线程
        if(job.getState().equals(State.BLOCKED)||job.getState().equals(State.TIMED_WAITING)){
            try {
                job.stop();
            } catch (Throwable e) {
                log.info("直接结束线程异常："+job.getName(), e);
            }
        }else{
            job.interrupt();
        }
    }

	/**
	* 将指定job复制到KettleUtils中的资源库 <br/>
	* @author jingma
	* @param jobName job名称
	* @param jobPath job路径
	* @param fromRepository 来源资源库
    * @param toRepository 目标资源库
	* @throws KettleException
	*/
	public static void jobCopy(String jobName,String jobPath,Repository fromRepository,
	        Repository toRepository) throws KettleException {
		JobMeta jobMeta = KettleUtils.loadJob(jobName,jobPath,fromRepository);
		for(JobEntryCopy jec:jobMeta.getJobCopies()){
			if(jec.isTransformation()){
				JobEntryTrans jet = (JobEntryTrans)jec.getEntry();
				transCopy(jet.getObjectName(), jet.getDirectory(),fromRepository,toRepository);
			}else if(jec.isJob()){
				JobEntryJob jej = (JobEntryJob)jec.getEntry();
				jobCopy(jej.getObjectName(),jej.getDirectory(),fromRepository,toRepository);
			}
		}
		jobMeta.setRepository(toRepository);
		jobMeta.setMetaStore(toRepository.getMetaStore());
		if(!isDirectoryExist(toRepository,jobPath)){
			//所在目录不存在则创建
		    toRepository.createRepositoryDirectory(toRepository.findDirectory("/"), jobPath);
		}
		KettleUtils.saveJob(toRepository,jobMeta);
	}

	/**
	* 将指定转换复制到KettleUtils中的资源库 <br/>
	* @author jingma
	* @param jobName 转换名称
	* @param jobPath 转换路径
    * @param fromRepository 来源资源库
    * @param toRepository 目标资源库
	* @throws KettleException
	*/
	public static void transCopy(String transName,String transPath,Repository fromRepository,
            Repository toRepository) throws KettleException {
		TransMeta tm = KettleUtils.loadTrans(transName, transPath, fromRepository);
		for(StepMeta sm:tm.getSteps()){
			if(sm.isJobExecutor()){
				JobExecutorMeta jem = (JobExecutorMeta)sm.getStepMetaInterface();
				jobCopy(jem.getJobName(),jem.getDirectoryPath(),fromRepository,toRepository);
			}
			else if(sm.getStepMetaInterface() instanceof TransExecutorMeta){
				TransExecutorMeta te = (TransExecutorMeta)sm.getStepMetaInterface();
				transCopy(te.getTransName(), te.getDirectoryPath(),fromRepository,toRepository);
			}
		}
		if(!isDirectoryExist(toRepository,transPath)){
			//所在目录不存在则创建
		    toRepository.createRepositoryDirectory(toRepository.findDirectory("/"), transPath);
		}
		tm.setRepository(toRepository);
		tm.setMetaStore(toRepository.getMetaStore());
		KettleUtils.saveTrans(toRepository,tm);
	}

    /**
    * 获取作业id <br/>
    * 获取当前作业所在目录是否有相同的作业
    * @author jingma
    * @param jm 当前作业
    * @return 存在则返回id，不存在则返回null
    */
    public static ObjectId getJobId(JobMeta jm){
        return getJobId(jm.getName(), jm.getRepositoryDirectory());
    }
    /**
    * 作业转换id <br/>
    * @author jingma
    * @param name 作业名称
    * @param repositoryDirectory 作业目录
    * @return 存在则返回id，不存在则返回null
    */
    public static ObjectId getJobId(String name,
            RepositoryDirectoryInterface repositoryDirectory) {
        try {
            return repository.getJobId(name, repositoryDirectory);
        } catch (KettleException e) {
            log.debug("获取作业id失败", e);
        }
        return null;
    }

    /**
    * 获取转换id <br/>
    * 获取当前转换所在目录是否有相同的转换
    * @author jingma
    * @param tm 当前转换
    * @return 存在则返回id，不存在则返回null
    */
    public static ObjectId getTransformationID(TransMeta tm){
        return getTransformationID(tm.getName(), tm.getRepositoryDirectory());
    }
    /**
    * 获取转换id <br/>
    * @author jingma
    * @param name 转换名称
    * @param repositoryDirectory 转换目录
    * @return 存在则返回id，不存在则返回null
    */
    public static ObjectId getTransformationID(String name,
            RepositoryDirectoryInterface repositoryDirectory) {
        try {
            return repository.getTransformationID(name, repositoryDirectory);
        } catch (KettleException e) {
            log.debug("获取转换id失败", e);
        }
        return null;
    }

    /**
    * 修复转换连接线 <br/>
    * @author jingma
    * @param tm 转换元数据
    */
    public static void repairTransHop(TransMeta tm) {
        for(int i=0;i<tm.nrTransHops();i++){
            TransHopMeta hop = tm.getTransHop(i);
            hop.setFromStep(tm.findStep(hop.getFromStep().getName()));
            hop.setToStep(tm.findStep(hop.getToStep().getName()));
        }
    }

    /**
    * 将来源对象的参数拷贝到目标对象，并根据要求修改 <br/>
    * @author jingma
    * @param target 要设置的目标对象
    * @param source 来源对象
    * @param params 要修改的参数
    */
    public static void setParams(NamedParams target,
            NamedParams source,Map<String, String> params) {
        //修改参数
        target.eraseParameters();
        try {
            for ( String key : source.listParameters() ) {
                String defaultVal = source.getParameterDefault( key );
                if(params.containsKey(key)){
                    defaultVal = params.get(key);
                }
                target.addParameterDefinition( key, defaultVal, 
                        source.getParameterDescription( key ) );
            }
        } catch (Exception e) {
            log.error("保存参数失败", e);
        }
    }
    public static void setParams(NamedParams target,JSONObject params) {
        try {
            for ( String key : params.keySet()) {
                JSONObject p = params.getJSONObject(key);
                target.addParameterDefinition( key, p.getString("默认值"), 
                        p.getString("参数名称"));
            }
        } catch (Exception e) {
            log.error("保存参数失败", e);
        }
    }

    /**
    * 修复JOB的连接线，克隆的job连接线不能显示 <br/>
    * @author jingma
    * @param jm job元数据
    */
    public static void repairHop(JobMeta jm) {
        for(JobHopMeta hop:jm.getJobhops()){
            hop.setFromEntry(jm.findJobEntry(hop.getFromEntry().getName()));
            hop.setToEntry(jm.findJobEntry(hop.getToEntry().getName()));
        }
    }
	
    /**
    * 获取参数 <br/>
    * @author jingma
    * @param vs 
    * @param key 参数名称
    * @return 值
    */
    public static String getProp(VariableSpace vs, String key){
        String value = vs.environmentSubstitute("${"+key+"}");
        if(value.startsWith("${")){
            return "";
        }else{
            return value;
        }
    }
    /**
    * 获取参数并解析为JSON对象 <br/>
    * @author jingma
    * @param vs 
    * @param key 参数名称
    * @return 值
    */
    public static JSONObject getPropJSONObject(VariableSpace vs, String key){
        String value = getProp(vs, key);
        if(StringUtil.isNotBlank(value)){
            return JSON.parseObject(value);
        }else{
            return null;
        }
    }
    /**
    * 获取根job <br/>
    * @author jingma
    * @param rootjob 
    * @return
    */
    public static Job getRootJob(Job rootjob) {
        while(rootjob!=null&&rootjob.getParentJob()!=null){
            rootjob = rootjob.getParentJob();
        }
        return rootjob;
    }
    /**
    * 获取根job <br/>
    * @author jingma
    * @param jee 
    * @return
    */
    public static Job getRootJob(JobEntryBase jee) {
        Job rootjob = jee.getParentJob();
        return getRootJob(rootjob);
    }
    /**
    * 获取根job <br/>
    * @author jingma
    * @param si 
    * @return
    */
    public static Job getRootJob(StepInterface si) {
        Job rootjob = si.getTrans().getParentJob();
        return getRootJob(rootjob);
    }
    /**
    * 获取根job的id <br/>
    * @author jingma
    * @param jee 
    * @return
    */
    public static String getRootJobId(JobEntryBase jee) {
        return getRootJob(jee).getObjectId().getId();
    }
    /**
    * 获取根job的id <br/>
    * @author jingma
    * @param si 
    * @return
    */
    public static String getRootJobId(StepInterface si) {
        Job rootjob = getRootJob(si);
        if(rootjob!=null){
            return rootjob.getObjectId().getId();
        }else{
            return null;
        }
    }
    /**
    * 获取根job的名称 <br/>
    * @author jingma
    * @param si 
    * @return
    */
    public static String getRootJobName(StepInterface si) {
        Job rootjob = getRootJob(si);
        if(rootjob!=null){
            return rootjob.getObjectName();
        }else{
            return null;
        }
    }

    /**
    * 生成转换图 <br/>
    * @author jingma
    * @param transMeta
    * @return
    * @throws Exception
    */
    public static BufferedImage generateTransformationImage( TransMeta transMeta ) throws Exception {
      float magnification = 1.0f;
      Point maximum = transMeta.getMaximum();
      maximum.multiply( magnification );

      SwingGC gc = new SwingGC( null, maximum, 32, 0, 0 );
      TransPainter transPainter =
        new TransPainter(
          gc, transMeta, maximum, null, null, null, null, null, new ArrayList<AreaOwner>(),
          new ArrayList<StepMeta>(), 32, 1, 0, 0, true, "Arial", 10 );
      transPainter.setMagnification( magnification );
      transPainter.buildTransformationImage();

      BufferedImage image = (BufferedImage) gc.getImage();

      return image;
    }

    /**
    * 生成作业图 <br/>
    * @author jingma
    * @param jobMeta
    * @return
    * @throws Exception
    */
    public static BufferedImage generateJobImage( JobMeta jobMeta ) throws Exception {
      float magnification = 1.0f;
      Point maximum = jobMeta.getMaximum();
      maximum.multiply( magnification );

      SwingGC gc = new SwingGC( null, maximum, 32, 0, 0 );
      JobPainter jobPainter =
        new JobPainter(
          gc, jobMeta, maximum, null, null, null, null, null, new ArrayList<AreaOwner>(),
          new ArrayList<JobEntryCopy>(), 32, 1, 0, 0, true, "Arial", 10 );
      jobPainter.setMagnification( magnification );
      jobPainter.drawJob();

      BufferedImage image = (BufferedImage) gc.getImage();

      return image;
    }

    /**
     * loadTrans:加载作业模板. <br/>
     * @author jingma
     * @param jobname 转换名称
     * @param directory 转换路径
     * @return 作业元数据
     * @throws KettleException 
     * @since JDK 1.6
     */
    public static JobMeta loadJobTP(String jobname, String directory) throws KettleException {
        Object o = tpMap.get(directory+jobname);
        JobMeta tp = null;
        if(o==null){
            tp = loadJob(jobname, directory, repository);
            tpMap.put(directory+jobname, tp);
        }else{
            tp = (JobMeta) o;
        }
        tp = (JobMeta) tp.realClone(false);
        repairHop(tp);
        return tp;
    }

    /**
     * loadTrans:加载转换模板. <br/>
     * @author jingma
     * @param transname 转换名称
     * @param directory 转换路径
     * @return 转换元数据
     * @throws KettleException 
     * @since JDK 1.6
     */
    public static TransMeta loadTransTP(String transname, String directory) throws KettleException {
        Object o = tpMap.get(directory+transname);
        TransMeta tp = null;
        if(o==null){
            tp = loadTrans(transname, directory, repository);
            tpMap.put(directory+transname, tp);
        }else{
            tp = (TransMeta) o;
        }
        tp = (TransMeta) tp.realClone(false);
        repairTransHop(tp);
        return tp;
    }
    /**
     * setRepository:设置资源库. <br/>
     * @author jingma
     * @param repository 外部注入资源库
     * @since JDK 1.6
     */
    public static void setRepository(Repository repository){
        KettleUtils.repository = repository;
    }
}

