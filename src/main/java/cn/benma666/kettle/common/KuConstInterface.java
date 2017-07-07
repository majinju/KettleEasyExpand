/**
* Project Name:eova
* Date:2016年5月24日
* Copyright (c) 2016, jingma All Rights Reserved.
*/

package cn.benma666.kettle.common;



/**
 * 常量 <br/>
 * date: 2016年5月24日 <br/>
 * @author jingma
 * @version 
 */
public interface KuConstInterface {
    
    /**
    * 缓存作业的文件资源库代码
    */
    String CACHE_FILE_REP = "cacheFileRep";
    
    /**
    * 定时类别-不需要定时
    */
    String SCHEDULER_TYPE_NOT_TIMING = "0";
    /**
    * 定时类别-时间间隔
    */
    String SCHEDULER_TYPE_TIME_INTERVAL = "1";
    /**
    * 定时类别-天
    */
    String SCHEDULER_TYPE_DAY = "2";
    /**
    * 定时类别-周
    */
    String SCHEDULER_TYPE_WEEK = "3";
    /**
    * 定时类别-月
    */
    String SCHEDULER_TYPE_MONTH = "4";
}
