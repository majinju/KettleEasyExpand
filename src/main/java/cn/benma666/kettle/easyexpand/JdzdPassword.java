/**
* Project Name:KettleUtil
* Date:2016年6月29日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettle.easyexpand;

import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * JINDU接口密码生成<br/>
 * date: 2016年6月29日 <br/>
 * @author jingma
 * @version 
 */
public class JdzdPassword extends EasyExpandRunBase{
    /**
    * 具体处理每一行数据
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#disposeRow(java.lang.Object[])
    */
    @Override
    protected void disposeRow(Object[] outputRow) {
        outputRow[getFieldIndex("PASSWORD")] = getPassword(outputRow[getFieldIndex("PASSWORD")].toString());
    }
    public static String getPassword(String pw) {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String nowDate = df.format(new Date());
        return getSha( pw+ nowDate);
    }
    
    public static String getSha(String str) {
        if(str == null || str.length() == 0) {
            return null;
        }
        char hexDigits[] = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
        try{
            MessageDigest mdTemp = MessageDigest.getInstance("SHA1");
            mdTemp.update(str.getBytes("UTF-8"));
            byte[] md = mdTemp.digest();
            int j = md.length;
            char buf[] = new char[j*2];
            int k = 0;
            for(int i=0; i<j; i++) {
                byte byte0 = md[i];
                buf[k++] = hexDigits[byte0 >>> 4 & 0xf];
                buf[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(buf);
        } catch(Exception e) {
            return null;
        }
    }
    /**
    * 
    * @see cn.benma666.kettle.steps.easyexpand.EasyExpandRunBase#init()
    */
    @Override
    protected void init() {
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
        JSONObject params = new JSONObject();
        return JSON.toJSONString(params, true);
    }
    
    public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {
        
    }
}
