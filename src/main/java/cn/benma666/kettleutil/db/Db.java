/**
* Project Name:KettleUtil
* Date:2016年6月21日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettleutil.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osjava.sj.loader.SJDataSource;
import org.pentaho.di.core.database.util.DatabaseUtil;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.trans.step.BaseStep;

import cn.benma666.constants.UtilConst;
import cn.benma666.kettleutil.common.KuConst;
import cn.benma666.myutils.StringUtil;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 数据库操作类 <br/>
 * date: 2016年6月21日 <br/>
 * @author jingma
 * @version 
 */
public class Db extends org.beetl.sql.core.db.Db{
    /**
    * 日志
    */
    private static Log log = LogFactory.getLog(Db.class);
    /**
    * 查询一般配置的sql语句
    */
    private static String FIND_GENERAL_CONFIG_SQL = 
            "select expand from metl_unify_dict d where d.ocode=? and d.dict_category=?";
    /**
    * 获取数据库操作对象 <br/>
    * @author jingma
    * @param dbCode
    * @return
    */
    public static Db use(String dbCode) {
        try {
            DataSource dataSource = ( new DatabaseUtil() ).getNamedDataSource( dbCode );
            return new Db(dataSource,dbCode);
        } catch (KettleException e) {
            log.error("获取数据库失败:"+dbCode, e);
        }
        return null;
    }
    /**
    * 获取数据库操作对象 <br/>
    * @author jingma
    * @param ku 
    * @param dbCode
    * @return
    */
    public static Db use(BaseStep ku, String dbCode) {
        try {
            DataSource dataSource = ( new DatabaseUtil() ).getNamedDataSource( dbCode );
            return new Db(dataSource,dbCode);
        } catch (KettleException e) {
            if(ku!=null){
                ku.logError("获取数据库失败:"+dbCode, e);
            }else{
                log.error("获取数据库失败:"+dbCode, e);
            }
        }
        return null;
    }
    /**
    * 获取数据库操作对象 <br/>
    * @author jingma
    * @param jee 
    * @param dbCode
    * @return
    */
    public static Db use(JobEntryBase jee, String dbCode) {
        try {
            DataSource dataSource = ( new DatabaseUtil() ).getNamedDataSource( dbCode );
            return new Db(dataSource,dbCode);
        } catch (KettleException e) {
            if(jee!=null){
                jee.logError("获取数据库失败:"+dbCode, e);
            }else{
                log.error("获取数据库失败:"+dbCode, e);
            }
        }
        return null;
    }
    
    public Db(DataSource dataSource,String dbCode) {
        super(dbCode, dataSource, getDbtypeByDatasource(dataSource));
    }
    /**
    * 根据数据源获取数据库类型 <br/>
    * @author jingma
    * @param dataSource
    * @return
    */
    public static String getDbtypeByDatasource(DataSource dataSource) {
        String dbType = null;
        if(dataSource instanceof DruidDataSource){
            dbType = ((DruidDataSource)dataSource).getDbType();
        }else if(dataSource instanceof SJDataSource){
            dbType = JdbcUtils.getDbType(
                    ((SJDataSource)dataSource).toString().split("::::")[1],
                    null);
        }
        return dbType;
    }
    /**
    * 查询一般配置 <br/>
    * @author jingma
    * @param configCode 配置代码
    * @return 具体配置的JSON对象
    */
    public JSONObject findGeneralConfig(String configCode){
        String expand = findFirst(FIND_GENERAL_CONFIG_SQL, configCode,
                UtilConst.DICT_CATEGORY_GENERAL_CONFIG).
                getString(UtilConst.FIELD_EXPAND);
        if(StringUtil.isNotBlank(expand)){
            return JSON.parseObject(expand);
        }else{
            return null;
        }
    }
    /**
    * 关闭数据库连接即相关预编译语句 <br/>
    * @author jingma
    * @param jee 
    * @param conn 数据库连接
    * @param preps 预编译语句
    */
    public static void closeConn(JobEntryBase jee,Connection conn,PreparedStatement... preps){
        for(PreparedStatement p:preps){
            if(p != null){
                try {
                    p.close();
                } catch (SQLException e) {
                    jee.logError("关闭预处理游标失败", e);
                }
            }
        }
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                if(jee!=null){
                    jee.logError("关闭数据库连接失败", e);
                }else{
                    log.error("关闭数据库连接失败", e);
                }
            }
        }
    }
}
