create table LTS_ADMIN_JOB_CLIENT_MONITOR
(
  id                    NUMBER(20) primary key,
  gmt_created           NUMBER(20),
  node_group            NVARCHAR2(64),
  identity              NVARCHAR2(64),
  submit_success_num    NUMBER(11),
  submit_failed_num     NUMBER(11),
  fail_store_num        NUMBER(11),
  submit_fail_store_num NUMBER(11),
  handle_feedback_num   NUMBER(11),
  timestamp_            NUMBER(20)
)