create table LTS_ADMIN_JVM_THREAD
(
  id                         NUMBER(20) primary key,
  gmt_created                NUMBER(20),
  identity                   NVARCHAR2(64),
  timestamp_                 NUMBER(20),
  node_type                  NVARCHAR2(32),
  node_group                 NVARCHAR2(64),
  daemon_thread_count        NUMBER(11),
  thread_count               NUMBER(11),
  total_started_thread_count NUMBER(11),
  dead_locked_thread_count   NUMBER(11),
  process_cpu_time_rate      NUMBER
)