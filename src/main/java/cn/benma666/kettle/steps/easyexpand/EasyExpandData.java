package cn.benma666.kettle.steps.easyexpand;

/**   
 * @Title: 数据类 
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
	
