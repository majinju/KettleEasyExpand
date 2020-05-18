/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.bdhc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;

import org.beetl.sql.core.OnConnection;
import org.beetl.sql.core.SQLManager;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import cn.benma666.kettle.mytuils.Db;
import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import cn.benma666.myutils.JsonResult;
import cn.benma666.web.WebInitInterface;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * 比对核查-增量比对-号码比对<br/>
 * 注意性能优化，本系统考虑十亿级比对号码支持
 * date: 2016年6月29日 <br/>
 * @author jingma
 * @version 
 */
public class ZlbdHmbd extends EasyExpandRunBase implements WebInitInterface{
    /**
    * 字段-核查证件号码
    */
    public static final String FIELD_HCZJHM = "hczjhm";
    /**
    * 字段-核查证件类型
    */
    public static final String FIELD_HCZJLX = "hczjlx";
    /**
    * 比对号码缓存<比对号码，对应备用信息>，初始化全量加载，之后定时增量追加<br/>
    * 当比中后没有查询到该比对号码对应的号码信息时则移除该号码。
    */
    public static Map<String, JSONObject> bhhmMap = new HashMap<String, JSONObject>();
    
    /**
    * 具体处理每一行数据
     * @return 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
    */
    @Override
    protected JsonResult disposeRow(Object[] outputRow) throws Exception{
        if(bhhmMap.containsKey(inputRow[ku.getInputRowMeta().indexOfValue("BDHM")])){
            RowMetaInterface irm = ku.getInputRowMeta();
            JSONObject zy = new JSONObject(100);
            for(int i=0;i<irm.size();i++){
                ValueMetaInterface vm = irm.getValueMeta(i);
                zy.put(vm.getName().toLowerCase(), inputRow[ku.getInputRowMeta().indexOfValue(vm.getName())]);
            }
            ku.logBasic("比中了："+zy);
            
            JSONObject h = bhhmMap.get(zy.getString("bdhm"));
            //查询出所有比中的号码
            List<JSONObject> hms = Db.use().find("bdhc.selectHmByHchm", 
                    Db.buildMap(h.getString(FIELD_HCZJLX),h.getString(FIELD_HCZJHM)));
            if(hms.isEmpty()){
                //该比对号码没有对应的比对号码了,移除该比对号码
                bhhmMap.remove(zy.getString("bdhm"));
                ku.logBasic("该比对号码已无效，移除比对号码："+h);
            }
            //逐个处理比中的比对号码，每个号码生成条比对记录
            for(JSONObject hm:hms){
                if(!BdhcUtil.isZy(getVariavle("HCFS"), getVariavle("ZYLB"), hm)){
                    continue;
                }
                Object[] or1 = new Object[2];
                or1[getFieldIndex("HMOBJ")] = hm;
                or1[getFieldIndex("ZYOBJ")] = zy;
                ku.putRow(data.outputRowMeta, or1);
            }
        }
        return success("99");
    }
    /**
    * 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#init()
    */
    @Override
    protected void init() {
        if(bhhmMap.isEmpty()){
            init(null);
        }
    }
    /**
    * 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#end()
    */
    @Override
    protected void end() {
        ku.logBasic("数据处理结束");
    }

    /**
     * 
     * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#getDefaultConfigInfo(org.pentaho.di.trans.TransMeta, java.lang.String)
     */
     @Override
     public String getDefaultConfigInfo(TransMeta transMeta, String stepName) throws Exception{
        //创建一个JSON对象，用于构建配置对象，避免直接拼字符串构建JSON字符串
        JSONObject params = new JSONObject();
        //返回格式化后的默认JSON配置参数，供使用者方便快捷的修改配置
        return JSON.toJSONString(params, true);
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        //添加输出到下一步的字段
        r.clear();
        addField(r,"HMOBJ",ValueMeta.TYPE_NONE,
                ValueMeta.TRIM_TYPE_NONE,origin,"号码对象");
        addField(r,"ZYOBJ",ValueMeta.TYPE_NONE,
                ValueMeta.TRIM_TYPE_NONE,origin,"资源对象");
    }
    /**
    * 比对系统启动时执行，可以配置为早于作业启动，从而可以做一些要求在作业启动前的初始化工作。
    * @see cn.benma666.web.WebInitInterface#init(javax.servlet.ServletContext)
    */
    @Override
    public void init(ServletContext arg0) {
        log.info("开始加载比对号码缓存数据");
        final SQLManager sm = Db.use().getSqlManager();
        sm.executeOnConnection(new OnConnection<Map<String, JSONObject>>() {
            @Override
            public Map<String, JSONObject> call(Connection conn) throws SQLException {
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sm.getSQLResult("bdhc.selectHmBdhm",null).jdbcSql);
                ResultSetMetaData md = rs.getMetaData();
                int colNum = md.getColumnCount();
                rs.next();
                //key值有序
                JSONObject hmyl = new JSONObject(true);
                //先获取字段信息，避免持续的字段获取及转换操作，以提供性能
                for(int i=1;i<=colNum;i++){
                    hmyl.put(md.getColumnName(i).toLowerCase(), rs.getObject(i));
                }
                bhhmMap.put(hmyl.getString(FIELD_HCZJLX)+"_"+hmyl.getString(FIELD_HCZJHM), hmyl);
                log.info("1-号码加载样例："+hmyl);
                int row = 2;
                JSONObject r = null;
                int i = 1;
                while(rs.next()){
                    r = new JSONObject();
                    i = 1;
                    for(String key:hmyl.keySet()){
                        r.put(key, rs.getObject(i++));
                    }
                    //以后可以考虑将比对号码对应的全部号码存在value中，但因为比中毕竟是少数，所以暂时不考虑
                    bhhmMap.put(r.getString(FIELD_HCZJLX)+"_"+r.getString(FIELD_HCZJHM), r);
                    log.debug((row++)+"-每行号码信息："+r);
                }
                rs.close();
                st.close();
                return bhhmMap;
            }
        });
        log.info("结束加载比对号码缓存数据,共计号码数："+bhhmMap.size());
    }
}
