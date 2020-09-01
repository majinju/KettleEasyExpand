
----------先用kettle创建默认资源库，然后执行如下语句，再执行kettle_test.sql导入本系统的资源库数据

--r_job表扩展信息
-- Add/modify columns 
alter table R_JOB add RUN_STATUS VARCHAR2(100) default 'Stopped';
alter table R_JOB add LAST_UPDATE VARCHAR2(14) default to_char(sysdate,'yyyymmddhh24miss');
alter table R_JOB add AUTO_RESTART_NUM VARCHAR2(10) default '0';
alter table R_JOB add REPOSITORY_CODE VARCHAR2(100) default 'KETTLE_DEFAULT';
alter table R_JOB add PROJECT_CODE VARCHAR2(500) default 'KM_LOCALHOST_82';
alter table R_JOB add OORDER NUMBER default 9999;
alter table R_JOB add ZLSJC VARCHAR2(14);
alter table R_JOB add TIMING VARCHAR2(100);
alter table R_JOB add LOG_LEVEL VARCHAR2(100) default '3';
alter table R_JOB add ZYLX VARCHAR2(32) default 'cgzy';
alter table R_JOB add GXSX NUMBER;
alter table R_JOB add RZSX NUMBER;
alter table R_JOB add BZSX NUMBER;
alter table R_JOB add SCWCSJ VARCHAR2(14);
alter table R_JOB add SCZXZT VARCHAR2(32);
alter table R_JOB add JCPL VARCHAR2(64);
alter table R_JOB add GZLJ VARCHAR2(255);
alter table R_JOB add SHELL VARCHAR2(4000);
alter table R_JOB add SJZT VARCHAR2(32);
alter table R_JOB add SQL VARCHAR2(4000);
alter table R_JOB add JS VARCHAR2(4000);
alter table R_JOB add KMLM VARCHAR2(255);
alter table R_JOB add KMPZ VARCHAR2(4000);
alter table R_JOB add LYDX VARCHAR2(32);
alter table R_JOB add MBDX VARCHAR2(32);
alter table R_JOB add LZMB VARCHAR2(255);
alter table R_JOB add SRZJ VARCHAR2(32);
alter table R_JOB add SCZJ VARCHAR2(32);
-- Add comments to the columns 
comment on column R_JOB.RUN_STATUS
  is '运行状态';
comment on column R_JOB.LAST_UPDATE
  is '最后更新时间';
comment on column R_JOB.AUTO_RESTART_NUM
  is '自动重启次数';
comment on column R_JOB.REPOSITORY_CODE
  is '资源库代码';
comment on column R_JOB.PROJECT_CODE
  is '运行在';
comment on column R_JOB.OORDER
  is '对象排序';
comment on column R_JOB.ZLSJC
  is '增量时间戳';
comment on column R_JOB.TIMING
  is '定时';
comment on column R_JOB.LOG_LEVEL
  is '日志级别';
comment on column R_JOB.ZYLX
  is '作业类型@OTHER_KETTLE_ZYLX';
comment on column R_JOB.GXSX
  is '更新时限;单位分钟，r_job中的最后更新时间更新时限';
comment on column R_JOB.RZSX
  is '日志时限;单位分钟，日志表更新时限';
comment on column R_JOB.BZSX
  is '标志时限;单位分钟，抽取标志位时限';
comment on column R_JOB.SCWCSJ
  is '上次完成时间';
comment on column R_JOB.SCZXZT
  is '上次执行状态';
comment on column R_JOB.JCPL
  is '监测频率';
comment on column R_JOB.GZLJ
  is '工作路径';
comment on column R_JOB.SHELL
  is 'shell脚本';
comment on column R_JOB.SJZT
  is '数据载体';
comment on column R_JOB.SQL
  is 'sql脚本';
comment on column R_JOB.JS
  is 'js脚本';
comment on column R_JOB.KMLM
  is 'KM类名';
comment on column R_JOB.KMPZ
  is 'KM配置';
comment on column R_JOB.LYDX
  is '来源对象';
comment on column R_JOB.MBDX
  is '目标对象';
comment on column R_JOB.LZMB
  is '流转模板';
comment on column R_JOB.SRZJ
  is '输入组件';
comment on column R_JOB.SCZJ
  is '输出组件';



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
  
create or replace view v_job_params as
select ja.id_job,ja.id_job_attribute id,
to_char(ja.value_str) as ocode,
to_char(ja1.value_str) as oname,
to_char(ja2.value_str) as PARAM_DEFAULT,
p.value,p.simple_spell,p.full_spell,nvl(p.update_date,'0000') update_date,ja.nr,p.oid
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

create or replace view v_job_trans_yy as
select j.id_job,
       j.name,
       to_char(j.description) description,
       je.id_jobentry,
       je.name je_name,
       to_char(ja.value_str) zhmc,
       zhlj
  from r_job j
 inner join r_jobentry je
    on j.id_job = je.id_job
 inner join r_jobentry_attribute ja
    on ja.id_jobentry = je.id_jobentry
 inner join (select je.id_jobentry, ja.code, to_char(ja.value_str) zhlj
               from r_job j
              inner join r_jobentry je
                 on j.id_job = je.id_job
              inner join r_jobentry_attribute ja
                 on ja.id_jobentry = je.id_jobentry
              where je.id_jobentry_type =
                    (select jt.id_jobentry_type
                       from r_jobentry_type jt
                      where jt.code = 'TRANS')
                and ja.code = 'dir_path') jad
    on jad.id_jobentry = je.id_jobentry
 where je.id_jobentry_type =
       (select jt.id_jobentry_type
          from r_jobentry_type jt
         where jt.code = 'TRANS')
   and ja.code = 'name'
   /*作业对转换的引用视图*/;
   
---清空数据
truncate table JOB_LOG;
truncate table JOB_PARAMS;
truncate table JOB_WARNING;
truncate table R_CLUSTER;
truncate table R_CLUSTER_SLAVE;
truncate table R_CONDITION;
truncate table R_DATABASE;
truncate table R_DATABASE_ATTRIBUTE;
truncate table R_DATABASE_CONTYPE;
truncate table R_DATABASE_TYPE;
truncate table R_DEPENDENCY;
truncate table R_DIRECTORY;
truncate table R_ELEMENT;
truncate table R_ELEMENT_ATTRIBUTE;
truncate table R_ELEMENT_TYPE;
truncate table R_JOB;
truncate table R_JOB_ATTRIBUTE;
truncate table R_JOB_HOP;
truncate table R_JOB_LOCK;
truncate table R_JOB_NOTE;
truncate table R_JOBENTRY;
truncate table R_JOBENTRY_ATTRIBUTE;
truncate table R_JOBENTRY_COPY;
truncate table R_JOBENTRY_DATABASE;
truncate table R_JOBENTRY_TYPE;
truncate table R_LOG;
truncate table R_LOGLEVEL;
truncate table R_NAMESPACE;
truncate table R_NOTE;
truncate table R_PARTITION;
truncate table R_PARTITION_SCHEMA;
truncate table R_REPOSITORY_LOG;
truncate table R_SLAVE;
truncate table R_STEP;
truncate table R_STEP_ATTRIBUTE;
truncate table R_STEP_DATABASE;
truncate table R_STEP_TYPE;
truncate table R_TRANS_ATTRIBUTE;
truncate table R_TRANS_CLUSTER;
truncate table R_TRANS_HOP;
truncate table R_TRANS_LOCK;
truncate table R_TRANS_NOTE;
truncate table R_TRANS_PARTITION_SCHEMA;
truncate table R_TRANS_SLAVE;
truncate table R_TRANS_STEP_CONDITION;
truncate table R_TRANSFORMATION;
truncate table R_USER;
truncate table R_VALUE;
truncate table R_VERSION;

