/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettle.easyexpand;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

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

import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;
import cn.benma666.myutils.JsonResult;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
* 端口监听 <br/>
* date: 2018年8月2日 <br/>
* @author jingma
* @version 
*/
public class PortListen extends EasyExpandRunBase{
    /**
    * 
    */
    private static final String LISTEN_PORT = "监听端口";

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
    * 具体处理每一行数据
     * @return 
     * @throws Exception 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
    */
    @Override
    protected JsonResult disposeRow(Object[] outputRow) throws Exception {
        DatagramSocket datagramSocket;
        try {
            datagramSocket = new DatagramSocket(configInfo.getIntValue(LISTEN_PORT));
        } catch (Exception e1) {
            ku.logBasic("端口已经占用："+configInfo.getIntValue(LISTEN_PORT));
            SyslogIF syslog = Syslog.getInstance("udp");
            syslog.getConfig().setHost("localhost");
            syslog.getConfig().setPort(configInfo.getIntValue(LISTEN_PORT));
            syslog.info("close");
            syslog.shutdown();
            datagramSocket = new DatagramSocket(configInfo.getIntValue(LISTEN_PORT));
        }
        try {
            while (true) {
                DatagramPacket packet = new DatagramPacket(new byte[5120], 5120);
                datagramSocket.receive(packet);
                String msg = new String(packet.getData(), 0, packet.getData().length);
                ku.logRowlevel(packet.getAddress() + "/" + packet.getPort() + ":" + msg);
                packet.setData("receive".getBytes("UTF-8"));
                if(Trans.STRING_HALTING.equals(ku.getTrans().getStatus())){
                    ku.logBasic("该转换已停止");
                    break;
                }
                outputRow = new Object[]{msg};
                ku.putRow(data.outputRowMeta, outputRow); 
                datagramSocket.send(packet);
            }
        } catch (Exception e) {
            ku.logError("端口监听错误", e);
        }finally{
            if(!datagramSocket.isClosed()){
                datagramSocket.close();
            }
        }
        return success("完成");
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
        //返回格式化后的默认JSON配置参数，供使用者方便快捷的修改配置
        return JSON.toJSONString(params, true);
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        //添加输出到下一步的字段
        addField(r,"LISTEN_DATA",ValueMeta.TYPE_STRING,ValueMeta.TRIM_TYPE_BOTH,origin,"监听数据");
    }
}
