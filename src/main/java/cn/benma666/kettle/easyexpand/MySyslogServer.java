/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettle.easyexpand;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.productivity.java.syslog4j.Syslog;
import org.productivity.java.syslog4j.SyslogIF;
import org.productivity.java.syslog4j.server.SyslogServer;
import org.productivity.java.syslog4j.server.SyslogServerConfigIF;
import org.productivity.java.syslog4j.server.SyslogServerEventHandlerIF;
import org.productivity.java.syslog4j.server.SyslogServerIF;

import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
* syslog服务 <br/>
* date: 2018年8月2日 <br/>
* @author jingma
* @version 
*/
public class MySyslogServer extends EasyExpandRunBase{
    /**
    * 
    */
    private static final String LISTEN_TYPE = "监听类型";

    /**
    * 
    */
    private static final String LISTEN_PORT = "监听端口";
    
    private SyslogServerIF serverIF = null;

    /**
    * 开始处理每一行数据 <br/>
    * @author jingma
    * @return 
    * @throws KettleException 
    */
    public boolean run() throws Exception{
        if (ku.first) {
            data.outputRowMeta = new RowMeta();
            getFields(data.outputRowMeta, ku.getStepname(), null, null, ku);
            ku.first = false;
            init();
        }
        //创建输出记录
        Object[] outputRow = new Object[data.outputRowMeta.size()];
        disposeRow(outputRow);
        //将该记录设置到下一步骤的读取序列中
        end();
        ku.setOutputDone();
        return false;
    }
    /**
    * 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#stopAll()
    */
    @Override
    public void stopAll() {
        serverIF.shutdown();
    }
    /**
    * 具体处理每一行数据
     * @throws Exception 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
    */
    @Override
    protected void disposeRow(Object[] outputRow) throws Exception {
        serverIF = SyslogServer.getInstance(configInfo.getString(LISTEN_TYPE));
        Thread jtxc = null;
        try {
            SyslogServerEventHandlerIF eventHandler = new MySyslogServerEventHandler(ku,data);
            SyslogServerConfigIF config = serverIF.getConfig();
            config.setHost("localhost");
            config.setPort(configInfo.getIntValue(LISTEN_PORT));
            config.addEventHandler(eventHandler);
            serverIF.initialize(configInfo.getString(LISTEN_TYPE),config);
//            serverIF.run();
            jtxc = new Thread(new Runnable() {
                
                @Override
                public void run() {
                    serverIF.run();
                }
            },configInfo.getIntValue(LISTEN_PORT)+"端口监听");
            jtxc.start();
        } catch (Exception e1) {
            ku.logBasic("端口已经占用："+configInfo.getIntValue(LISTEN_PORT));
            SyslogIF syslog = Syslog.getInstance(configInfo.getString(LISTEN_TYPE));
            syslog.getConfig().setHost("localhost");
            syslog.getConfig().setPort(configInfo.getIntValue(LISTEN_PORT));
            syslog.info("close");
            syslog.shutdown();
            serverIF.run();
        }
        try {
            while (true) {
              if(Trans.STRING_HALTING.equals(ku.getTrans().getStatus())||ku.isStopped()||!jtxc.isAlive()){
                  serverIF.shutdown();
                  ku.logBasic("该转换已停止");
                  break;
              }
              Thread.sleep(1000l);
            }
        } catch (Exception e) {
            ku.logError("", e);
        }
    }
    /**
    * 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#init()
    */
    @Override
    protected void init() {
        ku.logBasic("初始化插件");
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
        params.put(LISTEN_PORT, 514);
        params.put(LISTEN_TYPE, "udp");
        //返回格式化后的默认JSON配置参数，供使用者方便快捷的修改配置
        return JSON.toJSONString(params, true);
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        //添加输出到下一步的字段
        addField(r,"FACILITY",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"");
        addField(r,"LOG_DATE",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"日志时间");
        addField(r,"LOG_LEVEL",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"日志级别");
        addField(r,"LISTEN_DATA",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"日志数据");
    }
}
