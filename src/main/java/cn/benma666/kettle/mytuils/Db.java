/**
* Project Name:KettleUtil
* Date:2016年6月21日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettle.mytuils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.pentaho.di.core.database.util.DatabaseUtil;

import cn.benma666.constants.UtilConst;
import cn.benma666.exception.MyException;
import cn.benma666.iframe.CacheFactory;
import cn.benma666.web.SConf;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;

/**
 * 数据库操作类 <br/>
 * date: 2016年6月21日 <br/>
 * @author jingma
 * @version 
 */
public class Db extends cn.benma666.db.Db{
    /**
    * kettle数据源缓存
    */
    private static JSONObject kdbMap = CacheFactory.use("kdbCache", CacheFactory.TYPE_MEMORY);
    /**
    * 获取数据库操作对象 <br/>
    * @author jingma
    * @param dbCode
    * @return
    */
    public static Db use(String dbCode) {
        Db db = (Db) kdbMap.get(dbCode);
        if(db!=null){
            return db;
        }
        if(Db.db==null&&!UtilConst.DEFAULT.equals(dbCode)){
            //先尝试连接默认数据源，便于后续获取字典等信息
            use(UtilConst.DEFAULT);
        }
        //先获取自有数据源，找不到在尝试simplejndi
        cn.benma666.db.Db tdb = cn.benma666.db.Db.use(dbCode);
        DataSource dataSource = null;
        if(tdb==null){
            try {
                dataSource = DatabaseUtil.getDataSourceFromJndi( dbCode, new InitialContext() );
            } catch (Exception e) {
                //只需在jndi中配置sjsj数据库即可，其他数据源使用myservice中配置的数据源
                throw new MyException("获取数据源失败",e);
            }
        }else{
            dataSource = tdb.getDs();
        }
        if(!SConf.isInited()){
            //没初始化则进行初始化操作，此处进行非web场景的初始化
            SConf sc = new SConf();
            //设置加载配置的字典类别
            List<String> configCodeList = new ArrayList<String>();
            configCodeList.add("SYS_COMMON_APPCONFIG");
            configCodeList.add("SYS_KP_APPCONFIG");
            sc.setConfigCodeList(configCodeList);
            Properties properties = new Properties();
            InputStream in = null;
            try {
                //主动读取kettle的配置文件
                in = new FileInputStream(System.getProperty("KETTLE_HOME")+"/.kettle/kettle.properties");
                properties.load(in);
            } catch (Exception e) {
                throw new MyException("读取配置失败",e);
            }finally{
                if(in!=null){
                    try {
                        in.close();
                    } catch (IOException e) {
                        throw new MyException("读取配置失败",e);
                    }
                }
            }
            sc.processProperties(properties);
        }
        return new Db(dbCode,dataSource);
    }
    public Db(String dbCode,DataSource dataSource) {
        super(dbCode, (DruidDataSource) dataSource);
    }
    /**
    * 关闭全部数据源 <br/>
    * @author jingma
    */
    public static void closeAll(){
        Iterator<Entry<String, Object>> ei = kdbMap.entrySet().iterator();
        while(ei.hasNext()){
            Entry<String, Object> e = ei.next();
            ((Db)e.getValue()).close();
            ei.remove();
        }
        cn.benma666.db.Db.closeAll();
    }
}
