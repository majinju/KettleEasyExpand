package cn.benma666.kettle.steps.easyexpand;

import org.apache.commons.lang3.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
* Kettle工具类 <br/>
* date: 2016年6月20日 <br/>
* @author jingma
* @version 
*/
public class EasyExpand extends BaseStep implements StepInterface {

	private EasyExpandData data;
	private EasyExpandMeta meta;
	private EasyExpandRunBase kui;
	
	public EasyExpand(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
		super(s, stepDataInterface, c, t, dis);
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
		meta = (EasyExpandMeta) smi;
		data = (EasyExpandData) sdi;
		if(StringUtils.isNotBlank(meta.getClassName())){
            try {
                //实例化配置的类
                if(first){
                    kui = (EasyExpandRunBase) Class.forName(
                            environmentSubstitute(meta.getClassName())).newInstance();
                    kui.setKu(this);
                    kui.setMeta(meta,this);
                }
                kui.setData(data);
                return kui.run();
            } catch (Exception e) {
                setErrors(getErrors()+1);
                logError("运行失败,"+meta.getClassName()+","
                +environmentSubstitute(meta.getConfigInfo()), e);
                return defaultRun();
            }
		}else{
	        return defaultRun();
		}

	}

    /**
    * 默认运行方法 <br/>
    * @author jingma
    * @return
    * @throws KettleException
    * @throws KettleStepException
    */
    public boolean defaultRun() throws KettleException, KettleStepException {
        Object[] r = getRow(); // get row, blocks when needed!
		if (r == null) // no more input to be expected...
		{
			setOutputDone();
			return false;
		}

		if (first) {
			first = false;

			data.outputRowMeta = (RowMetaInterface) getInputRowMeta().clone();
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

			logBasic("template step initialized successfully");

		}
		
		Object[] outputRow = RowDataUtil.createResizedCopy( r, data.outputRowMeta.size() );

		putRow(data.outputRowMeta, outputRow); // copy row to possible alternate rowset(s)

		if (checkFeedback(getLinesRead())) {
			logBasic("Linenr " + getLinesRead()); // Some basic logging
		}

		return true;
    }

	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (EasyExpandMeta) smi;
		data = (EasyExpandData) sdi;

		return super.init(smi, sdi);
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (EasyExpandMeta) smi;
		data = (EasyExpandData) sdi;

		super.dispose(smi, sdi);
	}
}
