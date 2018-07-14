/**
* Project Name:KettleEasyExpand
* Date:2018年7月11日
* Copyright (c) 2018, jingma All Rights Reserved.
*/

package cn.benma666.test;

import org.junit.Test;
import org.pentaho.di.core.Const;

/**
 *  <br/>
 * date: 2018年7月11日 <br/>
 * @author jingma
 * @version 
 */
public class ExceptionTest {
    @SuppressWarnings("null")
    @Test
    public void getClassicStackTraceTest(){
        String[] xx = null;
        try {
            xx[4].charAt(1);
        } catch (Exception e) {
            System.out.println(Const.getClassicStackTrace(e));
        }
    }
}
