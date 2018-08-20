/**
 * Project Name:KettleEasyExpand
 * Date:2018年8月2日
 * Copyright (c) 2018, jingma All Rights Reserved.
 */

package cn.benma666.kettle.easyexpand;

import java.util.Arrays;
import java.util.Date;

import org.pentaho.di.core.exception.KettleStepException;
import org.productivity.java.syslog4j.server.SyslogServerEventHandlerIF;
import org.productivity.java.syslog4j.server.SyslogServerEventIF;
import org.productivity.java.syslog4j.server.SyslogServerIF;
import org.productivity.java.syslog4j.util.SyslogUtility;

import cn.benma666.kettle.steps.easyexpand.EasyExpand;
import cn.benma666.kettle.steps.easyexpand.EasyExpandData;
import cn.benma666.myutils.DateUtil;

/**
 * <br/>
 * date: 2018年8月2日 <br/>
 * 
 * @author jingma
 * @version
 */
public class MySyslogServerEventHandler implements SyslogServerEventHandlerIF {
    private static final long serialVersionUID = 6036413238696050746L;
    private EasyExpand ku;
    private EasyExpandData data;

    /**
    * Creates a new instance of MySyslogServerEventHandler.
    * @param ku
    * @param data
    */
    public MySyslogServerEventHandler(EasyExpand ku, EasyExpandData data) {
        this.ku = ku;
        this.data = data;
    }

    public void event(SyslogServerIF paramSyslogServerIF,
            SyslogServerEventIF paramSyslogServerEventIF) {
        String str1 = DateUtil.dateToStr14(paramSyslogServerEventIF.getDate() == null ? new Date()
                : paramSyslogServerEventIF.getDate());
        String str2 = SyslogUtility.getFacilityString(paramSyslogServerEventIF
                .getFacility());
        String str3 = SyslogUtility.getLevelString(paramSyslogServerEventIF
                .getLevel());
        Object[] outputRow = new Object[]{str2,str1,str3,paramSyslogServerEventIF.getMessage()};
        try {
            ku.putRow(data.outputRowMeta, outputRow);
            if(paramSyslogServerEventIF.getMessage().endsWith("close")){
                ku.logBasic("接到关闭指令，关闭消息接收");
                paramSyslogServerIF.shutdown();
            }
        } catch (KettleStepException e) {
            ku.logError("日志输出失败:"+Arrays.toString(outputRow), e);
        } 
    }

}
