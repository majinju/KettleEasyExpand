package cn.benma666.kettle.steps.easyexpand;

/**   
 * @Title: 数据类 
 * @Package plugin.template 
 * @Description: TODO(用一句话描述该文件做什么) 
 * @author http://www.ahuoo.com  
 * @date 2010-8-8 下午05:10:26 
 * @version V1.0   
 */

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class EasyExpandData extends BaseStepData implements StepDataInterface {

	public RowMetaInterface outputRowMeta;
	
    public EasyExpandData()
	{
		super();
	}
}
	
