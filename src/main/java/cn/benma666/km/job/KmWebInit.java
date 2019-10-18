/**
* Project Name:KettleEasyExpand
* Date:2019年10月16日
* Copyright (c) 2019, jingma All Rights Reserved.
*/

package cn.benma666.km.job;

import javax.servlet.ServletContext;

import cn.benma666.web.WebInitInterface;

/**
 * kettle在web应用中初始化 <br/>
 * date: 2019年10月16日 <br/>
 * @author jingma
 * @version 
 */
public class KmWebInit implements WebInitInterface{

    /**
    * 
    * @see cn.benma666.web.WebInitInterface#init(javax.servlet.ServletContext)
    */
    @Override
    public void init(ServletContext arg0) {
        JobManager.init();
    }
}
