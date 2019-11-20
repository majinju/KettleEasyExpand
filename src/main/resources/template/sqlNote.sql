
--r_job表扩展信息
ID_JOB	INTEGER	N			主键
ID_DIRECTORY	INTEGER	Y			目录
NAME	VARCHAR2(255)	Y			名称
DESCRIPTION	CLOB	Y			描述
EXTENDED_DESCRIPTION	CLOB	Y			扩展描述;json
JOB_VERSION	VARCHAR2(255)	Y			作业类别@KETTLE_ZYLB_YBFL
JOB_STATUS	INTEGER	Y			作业状态
ID_DATABASE_LOG	INTEGER	Y			日志数据库
TABLE_NAME_LOG	VARCHAR2(255)	Y			日志表
CREATED_USER	VARCHAR2(255)	Y			创建人
CREATED_DATE	DATE	Y			创建时间
MODIFIED_USER	VARCHAR2(255)	Y			修改人
MODIFIED_DATE	DATE	Y			修改时间
USE_BATCH_ID	CHAR(1)	Y			
PASS_BATCH_ID	CHAR(1)	Y			
USE_LOGFIELD	CHAR(1)	Y			
SHARED_FILE	VARCHAR2(255)	Y			
RUN_STATUS	VARCHAR2(100)	Y	'Stopped'		运行状态
LAST_UPDATE	VARCHAR2(14)	Y	to_char(sysdate,'yyyymmddhh24miss')		最后更新时间
AUTO_RESTART_NUM	VARCHAR2(10)	Y	'0'		自动重启次数
REPOSITORY_CODE	VARCHAR2(100)	Y	'KETTLE_DEFAULT'		资源库代码
PROJECT_CODE	VARCHAR2(500)	Y	'KM_LOCALHOST_82'		运行在
OORDER	NUMBER	Y	9999		对象排序
ZLSJC	VARCHAR2(14)	Y			增量时间戳
TIMING	VARCHAR2(100)	Y			定时
LOG_LEVEL	VARCHAR2(100)	Y	'3'		日志级别
ZYLX	VARCHAR2(32)	Y	'cgzy'		作业类型@OTHER_KETTLE_ZYLX
GXSX	NUMBER	Y			更新时限;单位分钟，r_job中的最后更新时间更新时限
RZSX	NUMBER	Y			日志时限;单位分钟，日志表更新时限
BZSX	NUMBER	Y			标志时限;单位分钟，抽取标志位时限
SCWCSJ	VARCHAR2(14)	Y			上次完成时间
SCZXZT	VARCHAR2(32)	Y			上次执行状态
JCPL	VARCHAR2(64)	Y			监测频率
GZLJ	VARCHAR2(255)	Y			工作路径
SHELL	VARCHAR2(4000)	Y			shell脚本
SJZT	VARCHAR2(32)	Y			数据载体
SQL	VARCHAR2(4000)	Y			sql脚本
JS	VARCHAR2(4000)	Y			js脚本
KMLM	VARCHAR2(255)	Y			KM类名
KMPZ	VARCHAR2(4000)	Y			KM配置
LYDX	VARCHAR2(32)	Y			来源对象
MBDX	VARCHAR2(32)	Y			目标对象
LZMB	VARCHAR2(255)	Y			流转模板
GDPZ	VARCHAR2(4000)	Y			更多配置
SRZJ	VARCHAR2(32)	Y			输入组件
SCZJ	VARCHAR2(32)	Y			输出组件


-- Create table
create table JOB_LOG
(
  OID          VARCHAR2(32) default sys_guid() not null,
  OCODE        VARCHAR2(100),
  ONAME        VARCHAR2(100),
  ODESCRIBE    VARCHAR2(500),
  OORDER       NUMBER,
  SIMPLE_SPELL VARCHAR2(200),
  FULL_SPELL   VARCHAR2(500),
  CREATE_DATE  VARCHAR2(14) default to_char(sysdate,'yyyymmddhh24miss'),
  UPDATE_DATE  VARCHAR2(14) default to_char(sysdate,'yyyymmddhh24miss'),
  CREATE_USER  VARCHAR2(100),
  UPDATE_USER  VARCHAR2(100),
  EXPAND       VARCHAR2(4000) default '{}',
  IS_DISABLE   VARCHAR2(10) default '0',
  FLAG1        VARCHAR2(200),
  FLAG2        VARCHAR2(200),
  ID_JOB       NUMBER,
  JOB_NAME     VARCHAR2(200),
  START_DATE   VARCHAR2(14),
  END_DATE     VARCHAR2(14),
  RESULT       VARCHAR2(200),
  LOG_FILE     VARCHAR2(1000)
);
-- Add comments to the table 
comment on table JOB_LOG
  is '作业日志';
-- Add comments to the columns 
comment on column JOB_LOG.OID
  is '对象主键';
comment on column JOB_LOG.OCODE
  is '对象代码';
comment on column JOB_LOG.ONAME
  is '对象名称';
comment on column JOB_LOG.ODESCRIBE
  is '对象描述';
comment on column JOB_LOG.OORDER
  is '对象排序';
comment on column JOB_LOG.SIMPLE_SPELL
  is '对象简拼';
comment on column JOB_LOG.FULL_SPELL
  is '对象全拼';
comment on column JOB_LOG.CREATE_DATE
  is '创建时间';
comment on column JOB_LOG.UPDATE_DATE
  is '更新时间';
comment on column JOB_LOG.CREATE_USER
  is '创建人';
comment on column JOB_LOG.UPDATE_USER
  is '更新人';
comment on column JOB_LOG.EXPAND
  is '扩展信息';
comment on column JOB_LOG.IS_DISABLE
  is '是否禁用';
comment on column JOB_LOG.FLAG1
  is '备用1';
comment on column JOB_LOG.FLAG2
  is '备用2';
comment on column JOB_LOG.ID_JOB
  is '作业ID';
comment on column JOB_LOG.JOB_NAME
  is '作业名称';
comment on column JOB_LOG.START_DATE
  is '开始时间';
comment on column JOB_LOG.END_DATE
  is '结束时间';
comment on column JOB_LOG.RESULT
  is '运行结果';
comment on column JOB_LOG.LOG_FILE
  is '日志文件';

  -- Create table
create table JOB_PARAMS
(
  OID          VARCHAR2(32) default sys_guid() not null,
  OCODE        VARCHAR2(100),
  ONAME        VARCHAR2(100),
  ODESCRIBE    VARCHAR2(500),
  OORDER       NUMBER,
  SIMPLE_SPELL VARCHAR2(200),
  FULL_SPELL   VARCHAR2(500),
  CREATE_DATE  VARCHAR2(14) default to_char(sysdate,'yyyymmddhh24miss'),
  UPDATE_DATE  VARCHAR2(14) default to_char(sysdate,'yyyymmddhh24miss'),
  CREATE_USER  VARCHAR2(100),
  UPDATE_USER  VARCHAR2(100),
  EXPAND       VARCHAR2(2000),
  IS_DISABLE   VARCHAR2(10) default '0',
  FLAG1        VARCHAR2(200),
  FLAG2        VARCHAR2(200),
  ID_JOB       NUMBER,
  VALUE        VARCHAR2(2000)
);
-- Add comments to the table 
comment on table JOB_PARAMS
  is '作业参数设置';
-- Add comments to the columns 
comment on column JOB_PARAMS.OID
  is '对象主键';
comment on column JOB_PARAMS.OCODE
  is '对象代码';
comment on column JOB_PARAMS.ONAME
  is '对象名称';
comment on column JOB_PARAMS.ODESCRIBE
  is '对象描述';
comment on column JOB_PARAMS.OORDER
  is '对象排序';
comment on column JOB_PARAMS.SIMPLE_SPELL
  is '对象简拼';
comment on column JOB_PARAMS.FULL_SPELL
  is '对象全拼';
comment on column JOB_PARAMS.CREATE_DATE
  is '创建时间';
comment on column JOB_PARAMS.UPDATE_DATE
  is '更新时间';
comment on column JOB_PARAMS.CREATE_USER
  is '创建人';
comment on column JOB_PARAMS.UPDATE_USER
  is '更新人';
comment on column JOB_PARAMS.EXPAND
  is '扩展信息';
comment on column JOB_PARAMS.IS_DISABLE
  is '是否禁用';
comment on column JOB_PARAMS.FLAG1
  is '备用1';
comment on column JOB_PARAMS.FLAG2
  is '备用2';
comment on column JOB_PARAMS.ID_JOB
  is '作业';
comment on column JOB_PARAMS.VALUE
  is '变量值';
-- Create/Recreate primary, unique and foreign key constraints 
alter table JOB_PARAMS
  add constraint PK_JOB_PARAMS primary key (OID);
-- Create/Recreate indexes 
create index IDX_JOB_PARAMS_CREATE_DATE on JOB_PARAMS (CREATE_DATE);
create index IDX_JOB_PARAMS_ONAME on JOB_PARAMS (ONAME);
create index IDX_JOB_PARAMS_UPDATE_DATE on JOB_PARAMS (UPDATE_DATE);

  -- Create table
create table JOB_WARNING
(
  OID          VARCHAR2(32) default sys_guid() not null,
  OCODE        VARCHAR2(100),
  ONAME        VARCHAR2(100),
  ODESCRIBE    VARCHAR2(500),
  OORDER       NUMBER,
  SIMPLE_SPELL VARCHAR2(200),
  FULL_SPELL   VARCHAR2(500),
  CREATE_DATE  VARCHAR2(14) default to_char(sysdate,'yyyymmddhh24miss'),
  UPDATE_DATE  VARCHAR2(14) default to_char(sysdate,'yyyymmddhh24miss'),
  CREATE_USER  VARCHAR2(100),
  UPDATE_USER  VARCHAR2(100),
  EXPAND       VARCHAR2(4000) default '{}',
  IS_DISABLE   VARCHAR2(10) default '0',
  FLAG1        VARCHAR2(200),
  FLAG2        VARCHAR2(200),
  ID_JOB       NUMBER,
  JOB_NAME     VARCHAR2(200),
  LOG_FILE     VARCHAR2(1000),
  MSG          VARCHAR2(4000),
  LOG_LEVEL    VARCHAR2(10),
  ERROR        VARCHAR2(10),
  SUBJECT      VARCHAR2(100),
  LOG_CHANNEL  VARCHAR2(100)
);
-- Add comments to the table 
comment on table JOB_WARNING
  is '作业预警';
-- Add comments to the columns 
comment on column JOB_WARNING.OID
  is '对象主键';
comment on column JOB_WARNING.OCODE
  is '对象代码';
comment on column JOB_WARNING.ONAME
  is '对象名称';
comment on column JOB_WARNING.ODESCRIBE
  is '对象描述';
comment on column JOB_WARNING.OORDER
  is '对象排序';
comment on column JOB_WARNING.SIMPLE_SPELL
  is '对象简拼';
comment on column JOB_WARNING.FULL_SPELL
  is '对象全拼';
comment on column JOB_WARNING.CREATE_DATE
  is '创建时间';
comment on column JOB_WARNING.UPDATE_DATE
  is '更新时间';
comment on column JOB_WARNING.CREATE_USER
  is '创建人';
comment on column JOB_WARNING.UPDATE_USER
  is '更新人';
comment on column JOB_WARNING.EXPAND
  is '扩展信息';
comment on column JOB_WARNING.IS_DISABLE
  is '是否禁用';
comment on column JOB_WARNING.FLAG1
  is '备用1';
comment on column JOB_WARNING.FLAG2
  is '备用2';
comment on column JOB_WARNING.ID_JOB
  is '作业ID';
comment on column JOB_WARNING.JOB_NAME
  is '作业名称';
comment on column JOB_WARNING.LOG_FILE
  is '日志文件';
comment on column JOB_WARNING.MSG
  is '预警日志';
comment on column JOB_WARNING.LOG_LEVEL
  is '日志级别';
comment on column JOB_WARNING.ERROR
  is '是否错误';
comment on column JOB_WARNING.SUBJECT
  is '宿主';
comment on column JOB_WARNING.LOG_CHANNEL
  is '日志通道';
  
  create or replace view kettle_test.v_job_params as
select ja.id_job,ja.id_job_attribute id,
to_char(ja.value_str) as ocode,
to_char(ja1.value_str) as oname,
to_char(ja2.value_str) as PARAM_DEFAULT,
p.value,p.simple_spell,p.full_spell,p.update_date
from r_job_attribute ja
inner join r_job_attribute ja1 on ja1.id_job=ja.id_job and ja1.nr=ja.nr and ja1.code='PARAM_DESC'
inner join r_job_attribute ja2 on ja2.id_job=ja.id_job and ja2.nr=ja.nr and ja2.code='PARAM_DEFAULT'
inner join r_job j on j.id_job=ja.id_job
left join job_params p on p.id_job=ja.id_job and to_char(ja.value_str)=p.ocode
where ja.code = 'PARAM_KEY'
order by ja.nr asc
/*
参数设置
*/;

