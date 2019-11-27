/**
* Project Name:kettle-manager
* Date:2017年6月13日
* Copyright (c) 2017, jingma All Rights Reserved.
*/

package cn.benma666.km.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.eval.JobEntryEval;
import org.pentaho.di.job.entries.job.JobEntryJob;
import org.pentaho.di.job.entries.shell.JobEntryShell;
import org.pentaho.di.job.entries.sql.JobEntrySQL;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.excelinput.ExcelInputField;
import org.pentaho.di.trans.steps.excelinput.ExcelInputMeta;
import org.pentaho.di.trans.steps.excelinput.SpreadSheetType;
import org.pentaho.di.trans.steps.excelwriter.ExcelWriterStepField;
import org.pentaho.di.trans.steps.excelwriter.ExcelWriterStepMeta;
import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;
import org.pentaho.di.trans.steps.textfileinput.TextFileInputField;
import org.pentaho.di.trans.steps.textfileinput.TextFileInputMeta;
import org.pentaho.di.trans.steps.textfileoutput.TextFileField;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputMeta;

import cn.benma666.domain.SysQxYhxx;
import cn.benma666.domain.SysSjglSjdx;
import cn.benma666.exception.MyException;
import cn.benma666.iframe.BasicObject;
import cn.benma666.iframe.DictManager;
import cn.benma666.kettle.jobentry.easyexpand.JobEntryEasyExpand;
import cn.benma666.kettle.mytuils.KettleUtils;
import cn.benma666.myutils.DesUtil;
import cn.benma666.myutils.FtpUtil;
import cn.benma666.myutils.JsonResult;
import cn.benma666.myutils.StringUtil;
import cn.benma666.sjgl.DefaultLjq;
import cn.benma666.sjgl.LjqInterface;
import cn.benma666.web.SConf;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * kettle服务类 <br/>
 * date: 2017年6月13日 <br/>
 * @author jingma
 * @version 
 */
public class KettleService extends BasicObject{
    //字段常量
    /**
    * 公共作业-处理前工作
    */
    private static final String GGZY_CLQGZ = "GGZY_CLQGZ";
    /**
    * 公共作业-处理后工作
    */
    private static final String GGZY_CLHGZ = "GGZY_CLHGZ";
    /**
    * 公共作业-处理失败工作
    */
    private static final String GGZY_CLSBGZ = "GGZY_CLSBGZ";
    private static final String FIELD_ZDYS_SFGX = "是否更新";
    private static final String FIELD_ZDYS_MBZD = "目标字段";
    private static final String FIELD_ZDYS_LYZD = "来源字段";
    private static final String FIELD_ZDYS_ZDLX = "字段类型";
    private static final String FIELD_ZDYS_ZDCD = "字段长度";
    private static final String FIELD_GXTJ_YSF = "运算符";

    //模板信息
    public static final String templateHz = "Template.kjb";

    /**
    * 日志
    */
    public static Log log = LogFactory.getLog(KettleService.class);
    /**
    * 当前km的数据对象
    */
    @SuppressWarnings("unused")
    private SysSjglSjdx sjdx;
    /**
    * 当前数据对象的参数集
    */
    @SuppressWarnings("unused")
    private JSONObject myParams; 
    /**
    * 当前用户
    */
    private SysQxYhxx user;
    /**
    * 具体记录
    */
    private JSONObject obj;
    
    //////////////////对象流转参数/////////////////////////////////
    //流转对象
    private JSONObject lydxParams;
    private JSONObject mbdxParams;
    private SysSjglSjdx lydx;
    private SysSjglSjdx mbdx;
    private String srzj;
    private String sczj;
    private String lzmb;
    //作业参数
    private JSONObject params = new JSONObject();
    //更多配置出来
    private JSONObject gdpz;
    //输入配置
    private JSONObject srpz;
    //输出配置
    private JSONObject scpz;
    private StepMeta scStep;
    private StepMeta srStep;
    private List<String> updateLookup = new ArrayList<String>();
    private List<String> updateStream = new ArrayList<String>();
    private List<Boolean> update = new ArrayList<Boolean>();
    private JSONObject srSjzt;
    private JSONObject scSjzt;
    //////////////////对象流转参数/////////////////////////////////
    
    public KettleService(SysSjglSjdx sjdx, JSONObject myParams) {
        super();
        this.sjdx = sjdx;
        this.myParams = myParams;
        user = (SysQxYhxx) myParams.get(LjqInterface.KEY_USER);
    }

    /**
    * 编辑作业基本信息 <br/>
    */
    private JsonResult editJobInfo() throws KettleException, KettleXMLException{
        JobMeta jm = null;
        if(StringUtil.isNotBlank(obj.getString("id_job"))){
            jm = KettleUtils.loadJob(obj.getString("id_job"));
        }else{
            //创建作业元对象
            jm = new JobMeta(SConf.getVal("kettle.template.dir")+obj.getString("zylx")+templateHz, KettleUtils.getInstanceRep());
            jm.setName(obj.getString("name"));
            //设置目录
            String directory = obj.getString("id_directory");
            RepositoryDirectoryInterface dir = KettleUtils.makeDirs(directory);
            jm.setRepositoryDirectory(dir);
            jm.setCreatedUser(user.getYhxm());
            jm.setCreatedDate(new Date());
        }
        //设置作业描述
        jm.setDescription(obj.getString("description"));
        jm.setExtendedDescription(obj.getString("extended_description"));
        jm.setModifiedUser(user.getYhxm());
        jm.setModifiedDate(new Date());
        return success("修改成功",jm);
    }

    /**
    * 设置作业的相对路径 <br/>
     * @return 
    */
    private JsonResult getJobInfo() throws KettleException {
        String dir = KettleUtils.getDirectory(obj.getLong("id_directory"));
        JobMeta jm = KettleUtils.loadJob(obj.getString("name"),obj.getLongValue("id_directory"));
        obj.put("id_directory",dir);
        return success("获取成功",jm);
    }

    public JsonResult editJobJavascript() throws Exception {
        JobMeta jm = (JobMeta) editJobInfo().getData();
        JobEntryEval js = (JobEntryEval) jm.findJobEntry(obj.getString("zylx")).getEntry();
        js.setScript(obj.getString("js"));
        KettleUtils.saveJob(jm);
        return success("修改成功",jm);
    }
    public JsonResult getJobJavascript() throws KettleException{
        JobMeta jm = (JobMeta) getJobInfo().getData();
        JobEntryEval js = (JobEntryEval) jm.findJobEntry(obj.getString("zylx")).getEntry();
        obj.put("js", js.getScript());
        return success("获取成功",obj);
    }
    
    /**
    * 编辑作业 <br/>
    * @author jingma
    * @param paraMap
    * @return
    * @throws KettleException 
    * @throws KettleXMLException 
    */
    public JsonResult editJobShell() throws Exception {
        JobMeta jm = (JobMeta) editJobInfo().getData();
        JobEntryShell shell = (JobEntryShell) jm.findJobEntry(obj.getString("zylx")).getEntry();
        String workPath = obj.getString("gzlj");
        if(StringUtil.isBlank(workPath)){
            workPath = "/tmp";
        }
        shell.setWorkDirectory(workPath);
        shell.setScript(obj.getString("shell"));
        KettleUtils.saveJob(jm);
        return success("修改成功",jm);
    }
    
    /**
    * 作业获取 <br/>
    * @author jingma
    * @param jobJson
    * @return
    * @throws KettleException
    */
    public JsonResult getJobShell() throws KettleException{
        JobMeta jm = (JobMeta) getJobInfo().getData();
        JobEntryShell shell = (JobEntryShell) jm.findJobEntry(obj.getString("zylx")).getEntry();
        obj.put("gzlj", shell.getWorkDirectory());
        obj.put("shell", shell.getScript());
        return success("获取成功",obj);
    }

    /**
    * 编辑作业 <br/>
    * @author jingma
    * @param paraMap
    * @return
    * @throws KettleException 
    * @throws KettleXMLException 
    */
    public JsonResult editJobSql() throws Exception {
        JobMeta jm = (JobMeta) editJobInfo().getData();
        String dbCode = obj.getString("sjzt");
        DatabaseMeta dm = KettleUtils.createDatabaseMetaByJndi(dbCode);
        JobEntrySQL sql = (JobEntrySQL) jm.findJobEntry(obj.getString("zylx")).getEntry();
        sql.setSQL(obj.getString("sql"));
        sql.setDatabase(dm);
        KettleUtils.saveJob(jm);
        return success("修改成功",jm);
    }
    
    /**
    * 作业获取 <br/>
    * @author jingma
    * @param jobJson
    * @return
    * @throws KettleException
    */
    public JsonResult getJobSql() throws KettleException{
        JobMeta jm = (JobMeta) getJobInfo().getData();
        JobEntrySQL sql = (JobEntrySQL) jm.findJobEntry(obj.getString("zylx")).getEntry();
        obj.put("sql", sql.getSQL());
        obj.put("sjzt", sql.getDatabase().getName());
        return success("获取成功",obj);
    }

    /**
    * 编辑作业 <br/>
    * @author jingma
    * @param paraMap
    * @return
    * @throws KettleException 
    * @throws KettleXMLException 
    */
    public JsonResult editJobKm() throws Exception {
        JobMeta jm = (JobMeta) editJobInfo().getData();
        JobEntryEasyExpand km = (JobEntryEasyExpand) jm.findJobEntry(obj.getString("zylx")).getEntry();
        km.setClassName(obj.getString("kmlm"));
        km.setConfigInfo(obj.getString("kmpz"));
        KettleUtils.saveJob(jm);
        return success("修改成功",jm);
    }
    
    /**
    * 作业获取 <br/>
    * @author jingma
    * @param jobJson
    * @return
    * @throws KettleException
    */
    public JsonResult getJobKm() throws KettleException{
        JobMeta jm = (JobMeta) getJobInfo().getData();
        JobEntryEasyExpand km = (JobEntryEasyExpand) jm.findJobEntry(obj.getString("zylx")).getEntry();
        obj.put("kmlm", km.getClassName());
        obj.put("kmpz", km.getConfigInfo());
        return success("获取成功",obj);
    }

    /**
    * 编辑对象流转作业 <br/>
    */
    public JsonResult editJobDxlz() throws Exception {
        //获取流转对象
        lydxParams = (JSONObject) DefaultLjq.getJcxxById(obj.getString("lydx")).getData();
        mbdxParams = (JSONObject) DefaultLjq.getJcxxById(obj.getString("mbdx")).getData();
        lydx = (SysSjglSjdx) lydxParams.get(LjqInterface.KEY_SJDX);
        mbdx = (SysSjglSjdx) mbdxParams.get(LjqInterface.KEY_SJDX);
        srzj = getSrzj().getMsg();
        sczj = getSczj().getMsg();
        lzmb = getLzmb().getMsg();
        //更多配置出来
        gdpz = JSONObject.parseObject(obj.getString("extended_description"));
        gdpz.put("lydx", obj.getString("lydx"));
        gdpz.put("mbdx", obj.getString("mbdx"));
        gdpz.put("srzj", srzj);
        gdpz.put("sczj", sczj);
        gdpz.put("lzmb", lzmb);
        lydxParams.put("gdpz", gdpz);
        srpz = gdpz.getJSONObject("输入配置");
        scpz = gdpz.getJSONObject("输出配置");
        srSjzt = DictManager.zdObjByDmByCache(LjqInterface.ZD_SYS_COMMON_SJZT, srpz.getString("数据载体"));
        //解密密码
        srSjzt.put("jmmm", DesUtil.decrypt(srSjzt.getString("mm"), SConf.getVal("sjzt.mm.ejmm")));
        scSjzt = DictManager.zdObjByDmByCache(LjqInterface.ZD_SYS_COMMON_SJZT, scpz.getString("数据载体"));
        scSjzt.put("jmmm", DesUtil.decrypt(scSjzt.getString("mm"), SConf.getVal("sjzt.mm.ejmm")));
        //加载模板
        JobMeta jm = null;
        TransMeta clzh = null;
        String directory = null;
        JSONObject lzmbObj = DictManager.zdObjByDmByCache("KETTLE_DXLZ_LZMB", lzmb);
        //获取目录
        if(StringUtil.isNotBlank(obj.getString("id_job"))){
            directory = obj.getString("id_directory");
            if(!obj.getBooleanValue("cxsc")){
                jm = KettleUtils.loadJob(obj.getString("id_job"));
                clzh = KettleUtils.loadTrans("处理转换",directory);
                //设置为以前的id
                clzh.setObjectId(KettleUtils.getTransformationID(clzh));
            }
        }else{
            directory = obj.getString("id_directory")+"/"+obj.getString("name");
        }
        if(jm==null){
            RepositoryDirectoryInterface dir = KettleUtils.makeDirs(directory);
            //创建作业元对象
            jm = KettleUtils.loadJobTP("模板作业","/template/对象流转");
            jm.setRepositoryDirectory(dir);
            jm.setName(obj.getString("name"));
            jm.setCreatedUser(user.getYhxm());
            jm.setCreatedDate(new Date());
            //设置为产品状态
            jm.setJobstatus(2);
            clzh = KettleUtils.loadTransTP("处理转换-"+lzmbObj.getString("mc"), 
                    "/template/对象流转");
            //设置
            clzh.setRepositoryDirectory(dir);
            clzh.setName("处理转换");
            if(StringUtil.isNotBlank(obj.getString("id_job"))){
                //设置为以前的id
                jm.setObjectId(new StringObjectId(obj.getString("id_job")));
                clzh.setObjectId(KettleUtils.getTransformationID("处理转换",dir));
            }else{
                jm.setObjectId(null);
                clzh.setObjectId(null);
            }
        }
        //作业参数处理
        for ( String key : jm.listParameters() ) {
            addParam(key, jm.getParameterDefault( key ), jm.getParameterDescription( key ));
        }
        //编辑时先清空，后续会根据需要依次添加
        setParam(GGZY_CLQGZ,"");
        setParam(GGZY_CLHGZ,"");
        setParam(GGZY_CLSBGZ,"");
        jm.eraseParameters();
        JSONArray paramArray = JSON.parseObject(lzmbObj.getString("kzxx")).getJSONArray("组件参数");
        for(JSONObject p:paramArray.toArray(new JSONObject[]{})){
            params.put(p.getString("参数代码"), p);
        }
        //加载步骤
        scStep = clzh.findStep("输出");
        srStep = clzh.findStep("输入");
        //新建流转时
        if(StringUtil.isBlank(obj.getString("id_job"))||obj.getBooleanValue("cxsc")){
            JSONObject srzjObj = DictManager.zdObjByDmByCache("KETTLE_DXLZ_SRZJ", srzj);
            JSONObject sczjObj = DictManager.zdObjByDmByCache("KETTLE_DXLZ_SCZJ", sczj);
            //根据配置更换输入输出组件
            TransMeta zjjh = KettleUtils.loadTransTP("处理转换-组件集合", "/template/对象流转");
            //输入组件
            StepMetaInterface srSMI = zjjh.findStep("输入-"+srzjObj.getString("mc")).getStepMetaInterface();
            srStep.setStepMetaInterface(srSMI);
            srStep.setStepID(PluginRegistry.getInstance().getPluginId(StepPluginType.class, srSMI));
            //输出组件
            StepMetaInterface scSMI = zjjh.findStep("输出-"+sczjObj.getString("mc")).getStepMetaInterface();
            scStep.setStepMetaInterface(scSMI);
            scStep.setStepID(PluginRegistry.getInstance().getPluginId(StepPluginType.class, scSMI));
            //组件参数添加
            paramArray = JSON.parseObject(srzjObj.getString("kzxx")).getJSONArray("组件参数");
            for(JSONObject p:paramArray.toArray(new JSONObject[]{})){
                params.put(p.getString("参数代码"), p);
            }
            paramArray = JSON.parseObject(sczjObj.getString("kzxx")).getJSONArray("组件参数");
            for(JSONObject p:paramArray.toArray(new JSONObject[]{})){
                params.put(p.getString("参数代码"), p);
            }
        }
        
        //字段映射
        JSONArray zdys = gdpz.getJSONArray("字段映射");
        for(JSONObject ys:zdys.toArray(new JSONObject[]{})){
            updateLookup.add(ys.getString(FIELD_ZDYS_MBZD).toUpperCase());
            updateStream.add(ys.getString(FIELD_ZDYS_LYZD).toUpperCase());
            update.add(ys.getBoolean(FIELD_ZDYS_SFGX));
        }
        //输入
        switch (lydx.getDxztlx()) {
        case JdbcUtils.ORACLE:
        case JdbcUtils.MYSQL:
            bzscSjksr();
            break;
        case "ftp":
            srSjzt = FtpUtil.paseFtpUrl(srSjzt);
            addParam("XZ_FTP_IP",srSjzt.getString("ip"),"下载FTPIP");
            addParam("XZ_FTP_YHM",srSjzt.getString("yhm"),"下载FTP用户名");
            addParam("XZ_FTP_MM",srSjzt.getString("jmmm"),"下载FTP密码");
            addParam("XZ_FTP_KZBM",srSjzt.getString("encodeing"),"下载FTP控制编码");
            addParam("XZ_FTP_YCML",lydx.getDxgs(),"下载FTP远程目录");
            addParam("XZ_FTP_TPF",lydx.getJtdx(),"下载FTP通配符");
            //追加ftp工作
            addParamVal(GGZY_CLQGZ,"ftp");
            setParam("SOURCE_DIR", "${FTP_LSGML}${JOB_NAME}/"+lydx.getDxgs());
            bzscBdwjsr();
            break;
        case "bdwj":
            setParam("SOURCE_DIR", srSjzt.getString("ljc")+lydx.getDxgs());
            bzscBdwjsr();
            break;
        default:
            throw new MyException("暂不支持该数据载体类型作为来源："+lydx.getDxztlx());
        }
        //输出
        switch (mbdx.getDxztlx()) {
        case JdbcUtils.ORACLE:
        case JdbcUtils.MYSQL:
            bzscSjksc();
            break;
        case "ftp":
            scSjzt = FtpUtil.paseFtpUrl(scSjzt);
            addParam("SC_FTP_IP",scSjzt.getString("ip"),"上传FTPIP");
            addParam("SC_FTP_YHM",scSjzt.getString("yhm"),"上传FTP用户名");
            addParam("SC_FTP_MM",scSjzt.getString("jmmm"),"上传FTP密码");
            addParam("SC_FTP_KZBM",scSjzt.getString("encodeing"),"上传FTP控制编码");
            addParam("SC_FTP_YCML",mbdx.getDxgs(),"上传FTP远程目录");
            addParam("SC_FTP_TPF",mbdx.getJtdx(),"上传FTP通配符");
            //添加工作：创建上传临时目录
            addParamVal(GGZY_CLQGZ,"cjsclsml");
            addParamVal(GGZY_CLHGZ,"ftp");
            setParam("TG_FILENAME", "${FTP_LSGML}${JOB_NAME}/${SC_FTP_YCML}"+"/"+obj.getString("name"));
            bzscBdwjsc();
            break;
        case "bdwj":
            //作业名称作为生成文件的前缀
            String tgFilename = scSjzt.getString("ljc");
            if(StringUtil.isNotBlank(mbdx.getDxgs())){
                tgFilename += (mbdx.getDxgs()+"/");
            }
            setParam("TG_FILENAME", tgFilename+obj.getString("name"));
            bzscBdwjsc();
            break;
        default:
            throw new MyException("暂不支持该数据载体类型作为目标："+lydx.getDxztlx());
        }
        //设置作业
        jm.setDescription(obj.getString("description"));
//        jm.setExtendedDescription(JSON.toJSONString(gdpz, true));
//        obj.put("extended_description", jm.getExtendedDescription());
        jm.setModifiedUser(user.getYhxm());
        jm.setModifiedDate(new Date());
        clzh.setModifiedUser(user.getYhxm());
        clzh.setModifiedDate(new Date());
        //设置转换
        KettleUtils.setParams(jm, params );
        //设置参数传递
        JobEntryJob ggzy = (JobEntryJob)jm.findJobEntry("公共作业").getEntry();
        List<String> cszList = new ArrayList<String>();
        for(String key:params.keySet()){
            cszList.add("${"+key+"}");
        }
        ggzy.parameters = params.keySet().toArray(new String[params.keySet().size()]);
        ggzy.parameterValues = cszList.toArray(new String[cszList.size()]);
        ggzy.parameterFieldNames = new String[cszList.size()];
        //保存
        KettleUtils.saveTrans(clzh);
        KettleUtils.saveJob(jm);
        return success("修改成功",jm);
    }

    /**
    * 步骤生成-本地文件输入 <br/>
    * @author jingma
    */
    private void bzscBdwjsr() {
        addParamVal(GGZY_CLHGZ,"bdwj");
        addParamVal(GGZY_CLSBGZ,"bdwj");
        setParam("SOURCE_TPF", lydx.getJtdx());
        JSONArray zdys = gdpz.getJSONArray("字段映射");
        switch (srzj) {
        case "wbwj":
            setParam("SOURCE_FGF", srpz.getString("分隔符"));
            TextFileInputMeta text = (TextFileInputMeta) srStep.getStepMetaInterface();
            text.setEnclosure(srpz.getString("文本限定符"));
            text.setNrHeaderLines(srpz.getIntValue("头部行数"));
            text.setEncoding(srpz.getString("编码方式"));
            List<TextFileInputField> tfl = new ArrayList<TextFileInputField>();
            for(JSONObject fo:zdys.toArray(new JSONObject[zdys.size()])){
                TextFileInputField field = new TextFileInputField();
                field.setName(fo.getString(FIELD_ZDYS_LYZD).toUpperCase());
                field.setType(ValueMeta.getType(fo.getString(FIELD_ZDYS_ZDLX)));
                field.setLength(fo.getIntValue(FIELD_ZDYS_ZDCD));
                field.setTrimType(ValueMeta.getTrimTypeByDesc("去掉左右两端空格"));
                tfl.add(field);
            }
            text.setInputFields(tfl.toArray(new TextFileInputField[tfl.size()]));
            break;
        case "excel":
            ExcelInputMeta excel = (ExcelInputMeta) srStep.getStepMetaInterface();
            if(lydx.getJtdx().endsWith(".xls")){
                excel.setSpreadSheetType(SpreadSheetType.JXL);
            }else if(lydx.getJtdx().endsWith(".xlsx")){
                excel.setSpreadSheetType(SpreadSheetType.POI);
            }else{
                throw new MyException("excel类型的对象的具体对象的通配符后缀必须是.xls或.xlsx");
            }
            excel.setSheetName(new String[]{srpz.getString("工作表名称")});
            excel.setStartRow(new int[]{srpz.getIntValue("起始行")});
            excel.setStartColumn(new int[]{srpz.getIntValue("起始列")});
            List<ExcelInputField> efl = new ArrayList<ExcelInputField>();
            for(JSONObject fo:zdys.toArray(new JSONObject[zdys.size()])){
                ExcelInputField field = new ExcelInputField();
                field.setName(fo.getString(FIELD_ZDYS_LYZD).toUpperCase());
                field.setType(ValueMeta.getType(fo.getString(FIELD_ZDYS_ZDLX)));
                //去掉两边空格
                field.setTrimType(3);
                efl.add(field);
            }
            excel.setField(efl.toArray(new ExcelInputField[efl.size()]));
            break;
        default:
            throw new MyException("不支持的输入组件："+srzj);
        }
    }

    /**
    * 步骤生成-本地文件输出 <br/>
    * @author jingma
    */
    private void bzscBdwjsc() {
        JSONArray zdys = gdpz.getJSONArray("字段映射");
        switch (sczj) {
        case "wbwj":
            setParam("TG_FGF", scpz.getString("分隔符"));
            TextFileOutputMeta text = (TextFileOutputMeta) scStep.getStepMetaInterface();
            text.setEnclosure(scpz.getString("文本限定符"));
            text.setEncoding(scpz.getString("编码方式"));
            List<TextFileField> tfl = new ArrayList<TextFileField>();
            for(JSONObject fo:zdys.toArray(new JSONObject[zdys.size()])){
                TextFileField field = new TextFileField();
                field.setName(fo.getString(FIELD_ZDYS_LYZD).toUpperCase());
                field.setType(ValueMeta.getType(fo.getString(FIELD_ZDYS_ZDLX)));
                field.setLength(-1);
                field.setPrecision(-1);
                if("Number".equals(fo.getString(FIELD_ZDYS_ZDLX))){
                    field.setFormat("0.#####");
                }
                field.setTrimType(ValueMeta.getTrimTypeByDesc("去掉左右两端空格"));
                tfl.add(field);
            }
            text.setOutputFields(tfl.toArray(new TextFileField[tfl.size()]));
            break;
        case "excel":
            setParam("TG_SHEET", scpz.getString("工作表名称"));
            ExcelWriterStepMeta excel = (ExcelWriterStepMeta) scStep.getStepMetaInterface();
            if(mbdx.getJtdx().endsWith(".xls")){
                excel.setExtension("xls");
            }else if(mbdx.getJtdx().endsWith(".xlsx")){
                excel.setExtension("xlsx");
            }else{
                throw new MyException("excel类型的对象的具体对象的通配符后缀必须是.xls或.xlsx");
            }
            List<ExcelWriterStepField> efl = new ArrayList<ExcelWriterStepField>();
            for(JSONObject fo:zdys.toArray(new JSONObject[zdys.size()])){
                ExcelWriterStepField field = new ExcelWriterStepField();
                field.setName(fo.getString(FIELD_ZDYS_LYZD).toUpperCase());
                field.setType(ValueMeta.getType(fo.getString(FIELD_ZDYS_ZDLX)));
                efl.add(field);
            }
            excel.setOutputFields(efl.toArray(new ExcelWriterStepField[efl.size()]));
            break;
        default:
            throw new MyException("不支持的输出组件："+sczj);
        }
    }

    /**
    * 步骤生成-数据库输入 <br/>
    */
    private void bzscSjksr() throws Exception{
        TableInputMeta ti = (TableInputMeta) srStep.getStepMetaInterface();
        //输入的数据库
        DatabaseMeta lydb = KettleUtils.createDatabaseMetaByJndi(srpz.getString("数据载体"));
        ti.setDatabaseMeta(lydb);
        setParam("SOURCE_SQL", DefaultLjq.getDefaultSql(lydx, "cqsql", lydxParams).getMsg());
        if("zlmb".equals(gdpz.getString("lzmb"))){
            //增量模板时，处理前工作添加zl
            addParamVal(GGZY_CLQGZ,"zl");
        }
    }

    /**
    * 步骤生成-数据库输出 <br/>
    * @author jingma
    */
    private void bzscSjksc() throws Exception {
        setParam("TG_SCHEMA",mbdx.getDxgs());
        setParam("TG_TABLE",mbdx.getJtdx());
        //输出的数据库
        String pltjl = scpz.getString("输出批量提交量");
        DatabaseMeta mbdb = KettleUtils.createDatabaseMetaByJndi(scpz.getString("数据载体"));
        switch (sczj) {
        case "crgx":
            InsertUpdateMeta iu = (InsertUpdateMeta) scStep.getStepMetaInterface();
            iu.setCommitSize(pltjl);
            iu.setUpdateBypassed(scpz.getBoolean("输出不执行更新"));
            iu.setDatabaseMeta(mbdb);
            //设置字段映射
            iu.setUpdateLookup(updateLookup.toArray(new String[]{}));
            iu.setUpdateStream(updateStream.toArray(new String[]{}));
            iu.setUpdate(update.toArray(new Boolean[]{}));
            //设置去重条件
            List<String> keyLookup = new ArrayList<String>();
            List<String> keyStream = new ArrayList<String>();
            List<String> keyCondition = new ArrayList<String>();
            JSONObject[] gxtjs = gdpz.getJSONArray("更新条件").toArray(new JSONObject[]{});
            if(gxtjs.length==0){
                throw new MyException("插入更新模式必须设置更新条件");
            }
            for(JSONObject ys:gxtjs){
                keyLookup.add(ys.getString(FIELD_ZDYS_MBZD).toUpperCase());
                keyStream.add(ys.getString(FIELD_ZDYS_LYZD).toUpperCase());
                keyCondition.add(ys.getString(FIELD_GXTJ_YSF));
            }
            iu.setKeyLookup(keyLookup.toArray(new String[]{}));
            iu.setKeyStream(keyStream.toArray(new String[]{}));
            iu.setKeyStream2(new String[keyStream.size()]);
            iu.setKeyCondition(keyCondition.toArray(new String[]{}));
            break;
        case "bsc":
            TableOutputMeta to = (TableOutputMeta) scStep.getStepMetaInterface();
            to.setCommitSize(pltjl);
            to.setDatabaseMeta(mbdb);
            to.setTruncateTable(scpz.getBooleanValue("裁剪表"));
            //设置字段映射
            to.setFieldDatabase(updateLookup.toArray(new String[]{}));
            to.setFieldStream(updateStream.toArray(new String[]{}));
            break;
        default:
            throw new MyException("不支持的输出模式："+sczj);
        }
    }

    /**
    * 追加参数值 <br/>
    * @author jingma
    * @param csdm
    * @param mrz
    */
    private void addParamVal(String csdm, String mrz) {
        JSONObject p = params.getJSONObject(csdm);
        p.put("默认值", p.getString("默认值")+mrz+",");
    }
    /**
    * 设置参数 <br/>
    * @author jingma
    * @param csdm
    * @param mrz
    */
    private void setParam(String csdm, String mrz) {
        params.getJSONObject(csdm).put("默认值", mrz);
    }

    /**
    * 添加作业参数 <br/>
    * @author jingma
    * @param csdm
    * @param mrz
    * @param csmc
    */
    private void addParam(String csdm, String mrz, String csmc) {
        JSONObject p = new JSONObject();
        p.put("参数代码", csdm);
        p.put("参数名称", csmc);
        p.put("默认值", mrz);
        params.put(csdm, p);
    }

    /**
    * 获取流转模板 <br/>
    */
    private JsonResult getLzmb() {
        String lzmb = obj.getString("lzmb");
        if(StringUtil.isNotBlank(lzmb)){
            return success(lzmb);
        }
        switch (lydx.getDxztlx()) {
        case JdbcUtils.ORACLE:
        case JdbcUtils.MYSQL:
            lzmb = "zlmb";
            break;
        case "ftp":
        case "bdwj":
            lzmb = "mrmb";
            break;
        default:
            throw new MyException("暂不支持该数据载体类型作为来源："+lydx.getDxztlx());
        }
        return success(lzmb);
    }

    /**
    * 获取输入组件 <br/>
    */
    private JsonResult getSrzj() {
        String srzj = obj.getString("srzj");
        if(StringUtil.isNotBlank(srzj)){
            return success(srzj);
        }
        JSONObject lzpz = JSON.parseObject(DictManager.zdObjByDmByCache("SYS_SJGL_DXLX", lydx.getDxlx()).
                getString("kzxx")).getJSONObject("流转配置");
        if(lzpz==null||lzpz.getString("输入组件")==null){
            throw new MyException("该对象类型没有配置默认输入组件，请在字典管理中配置："+lydx.getDxlx());
        }
        srzj = lzpz.getString("输入组件");
        return success(srzj);
    }

    /**
    * 获取输出组件 <br/>
    */
    private JsonResult getSczj() {
        String sczj = obj.getString("sczj");
        if(StringUtil.isNotBlank(sczj)){
            return success(sczj);
        }
        JSONObject lzpz = JSON.parseObject(DictManager.zdObjByDmByCache("SYS_SJGL_DXLX", mbdx.getDxlx()).
                getString("kzxx")).getJSONObject("流转配置");
        if(lzpz==null||lzpz.getString("输出组件")==null){
            throw new MyException("该对象类型没有配置默认输出组件，请在字典管理中配置："+mbdx.getDxlx());
        }
        sczj = lzpz.getString("输出组件");
        return success(sczj);
    }

    /**
    * 获取对象流转默认配置 <br/>
    */
    public JsonResult getDxlzMrpz() {
        //获取基本信息
        lydxParams = (JSONObject) DefaultLjq.getJcxxById(obj.getString("lydx")).getData();
        mbdxParams = (JSONObject) DefaultLjq.getJcxxById(obj.getString("mbdx")).getData();
        lydx = (SysSjglSjdx) lydxParams.get(LjqInterface.KEY_SJDX);
        mbdx = (SysSjglSjdx) mbdxParams.get(LjqInterface.KEY_SJDX);
        srzj = getSrzj().getMsg();
        sczj = getSczj().getMsg();
        lzmb = getLzmb().getMsg();
        //创建更多配置对象
        gdpz = new JSONObject();
        //处理字段映射
        getMrzdys();
        //来源对象处理
        gdpz.put("输入配置", lydxMrpz());
        //目标对象处理
        gdpz.put("输出配置", mbdxMrpz());
        
        JSONObject r = new JSONObject();
        r.put("extended_description", gdpz.toJSONString());
        r.put("lzmb", lzmb);
        r.put("sczj", sczj);
        r.put("srzj", srzj);
        return success("获取配置成功", r);
    }

    /**
    * 目标对象默认配置 <br/>
    */
    private JSONObject mbdxMrpz() {
        JSONObject scpz = new JSONObject();
        scpz.put("数据载体", mbdx.getDxzt());
        //获取目标对象基础配置
        scpz.putAll(DefaultLjq.getJcpz(mbdxParams));
        //获取组件默认配置
        JSONObject sczjObj = DictManager.zdObjByDmByCache("KETTLE_DXLZ_SCZJ", sczj);
        scpz.putAll(getZjpz(sczjObj));
        return scpz;
    }

    /**
    * 来源对象默认配置 <br/>
    */
    private JSONObject lydxMrpz() {
        JSONObject srpz = new JSONObject();
        srpz.put("数据载体", lydx.getDxzt());
        //获取目标对象基础配置
        srpz.putAll(DefaultLjq.getJcpz(lydxParams));
        //获取组件默认配置
        JSONObject srzjObj = DictManager.zdObjByDmByCache("KETTLE_DXLZ_SRZJ", srzj);
        srpz.putAll(getZjpz(srzjObj));
        return srpz;
    }

    /**
    * 获取组件配置 <br/>
    * @author jingma
    * @param zjObj
    * @return
    */
    private JSONObject getZjpz(JSONObject zjObj) {
        JSONObject zjpz = (JSONObject)StringUtil.getJsonKeys(zjObj.getString("kzxx"), "组件配置");
        if(zjpz==null){
            zjpz = new JSONObject();
        }
        return zjpz;
    }

    /**
    * 获取默认字段映射 <br/>
    */
    @SuppressWarnings("unchecked")
    private void getMrzdys() {
        JSONArray zdys = new JSONArray();
        JSONArray gxtj = new JSONArray();
        Map<String,JSONObject> lyFields = (Map<String, JSONObject>) lydxParams.get(LjqInterface.KEY_FIELDS);
        Map<String,JSONObject> mbFields = (Map<String, JSONObject>) mbdxParams.get(LjqInterface.KEY_FIELDS);
        Map<String,JSONObject> lyFieldsByid = new HashMap<String,JSONObject>();
        Map<String,JSONObject> lyFieldsByBzzd = new HashMap<String,JSONObject>();
        for(JSONObject ly:lyFields.values()){
            lyFieldsByid.put(ly.getString("id"),ly);
            lyFieldsByBzzd.put(ly.getString("bzzd"),ly);
        }
        for(Entry<String, JSONObject> mbe:mbFields.entrySet()){
            JSONObject mbf = mbe.getValue();
            if("99".equals(mbf.getString("zdywlb"))){
                //跳过虚拟字段
                continue;
            }
            //字段代码相同
            JSONObject lyf = lyFields.get(mbe.getKey());
            if(lyf==null){
                //目标字段的标准字段在来源对象中
                lyf = lyFieldsByid.get(mbf.getString("bzzd"));
                if(lyf==null){
                    //目标字段的是来源对象字段的标准字段
                    lyf = lyFieldsByBzzd.get(mbf.getString("id"));
                }
            }
            if(lyf!=null){
                JSONObject f = new JSONObject();
                f.put(FIELD_ZDYS_LYZD, lyf.getString("zddm"));
                f.put(FIELD_ZDYS_MBZD, mbf.getString("zddm"));
                f.put(FIELD_ZDYS_SFGX, mbf.getBoolean("yxbj"));
                String zdlx = mbf.getString("zdlx");
                String nzdlx = DictManager.zdMcByDm("KETTLE_DXLZ_ZDLXYS", zdlx);
                if(zdlx.equals(nzdlx)){
                    nzdlx = "String";
                }
                f.put(FIELD_ZDYS_ZDLX, nzdlx);
                f.put(FIELD_ZDYS_ZDCD, mbf.getString("zdcd"));
                zdys.add(f);
                //更新条件判断
                if(mbf.getIntValue("qcbh")>0){
                    //去重编号大于0表示为去重字段
                    //更新条件
                    JSONObject tj = new JSONObject();
                    tj.put(FIELD_ZDYS_LYZD, lyf.getString("zddm"));
                    tj.put(FIELD_ZDYS_MBZD, mbf.getString("zddm"));
                    tj.put(FIELD_GXTJ_YSF, "=");
                    gxtj.add(tj);
                }
            }
            if(gxtj.size()==0){
                //当没有单独配置去重字段时，采用主键去重
                JSONObject tj = new JSONObject();
                tj.put(FIELD_ZDYS_LYZD, lydx.getZjzd());
                tj.put(FIELD_ZDYS_MBZD, mbdx.getZjzd());
                tj.put(FIELD_GXTJ_YSF, "=");
                gxtj.add(tj);
            }
            
        }
        gdpz.put("字段映射", zdys);
        gdpz.put("更新条件", gxtj);
    }

    public JsonResult getJobDxlz() throws KettleException{
        getJobInfo();
        return success("获取成功",obj);
    }

    /**
    * 导入作业 <br/>
    */
    public JsonResult impJob() {
        return error("暂不支持");
    }

    /**
    * 导入转换 <br/>
    */
    public static JsonResult impTrans() {
        return error("暂不支持");
    }
    
    /**
     * @return obj 
     */
    public JSONObject getObj() {
        return obj;
    }

    /**
     * @param obj the obj to set
     */
    public void setObj(JSONObject obj) {
        this.obj = obj;
    }

}
