create table LTS_ADMIN_JOB_TRACKER_MONITOR
(
  id                    NUMBER(20) primary key,
  gmt_created           NUMBER(20),
  identity              NVARCHAR2(64),
  receive_job_num       NUMBER(11),
  push_job_num          NUMBER(11),
  exe_success_num       NUMBER(11),
  exe_failed_num        NUMBER(11),
  exe_later_num         NUMBER(11),
  exe_exception_num     NUMBER(11),
  fix_executing_job_num NUMBER(11),
  timestamp_            NUMBER(20)
)