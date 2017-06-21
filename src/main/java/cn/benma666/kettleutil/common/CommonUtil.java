/**
* Project Name:KettleUtil
* Date:2016年6月21日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettleutil.common;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;

import cn.benma666.constants.UtilConst;
import cn.benma666.kettleutil.db.Db;
import cn.benma666.mytuils.KettleUtils;

import com.alibaba.fastjson.JSONObject;

/**
 * 一般工具类 <br/>
 * date: 2016年6月21日 <br/>
 * @author jingma
 * @version 
 */
public class CommonUtil {

    /**
    * 获取或创建指定代码的数据库 <br/>
    * 若不存在则自动根据metl系统配置在kettle中创建该数据库
    * @author jingma
    * @param dbCode 数据代码
    * @return 
    * @throws KettleException 
    */
    public static DatabaseMeta getOrCreateDB(String dbCode) throws KettleException {
        ObjectId dbId = null;
        Repository repository = KettleUtils.getInstanceRep();
        dbId = repository.getDatabaseID(dbCode);
        if(dbId==null){
            JSONObject metlDb = Db.use(UtilConst.DATASOURCE_METL).
                    findFirst("select * from metl_database db where db.ocode=?", dbCode);
            DatabaseMeta dataMeta = new DatabaseMeta(dbCode, KettleUtils.dbTypeToKettle(metlDb.getString("type")), 
                    DatabaseMeta.dbAccessTypeCode[DatabaseMeta.TYPE_ACCESS_JNDI], null, dbCode, null, null, null);
            KettleUtils.saveRepositoryElement(dataMeta);
            dbId = repository.getDatabaseID(dbCode);
        }
        return repository.loadDatabaseMeta(dbId, null);
    }
}
