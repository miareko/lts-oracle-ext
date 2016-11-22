create table LTS_ADMIN_TASK_TRACKER_MONITOR
(
  id                 NUMBER(20) primary key,
  gmt_created        NUMBER(20),
  node_group         NVARCHAR2(64),
  identity           NVARCHAR2(64),
  exe_success_num    NUMBER(11),
  exe_failed_num     NUMBER(11),
  exe_later_num      NUMBER(11),
  exe_exception_num  NUMBER(11),
  total_running_time NUMBER(20),
  timestamp_         NUMBER(20)
)