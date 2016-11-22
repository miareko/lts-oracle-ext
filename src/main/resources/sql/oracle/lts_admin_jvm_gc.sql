create table LTS_ADMIN_JVM_GC
(
  id                             NUMBER(20) primary key,
  gmt_created                    NUMBER(20),
  identity                       NVARCHAR2(64),
  timestamp_                     NUMBER(20),
  node_type                      NVARCHAR2(32),
  node_group                     NVARCHAR2(64),
  young_gc_collection_count      NUMBER(20),
  young_gc_collection_time       NUMBER(20),
  full_gc_collection_count       NUMBER(20),
  full_gc_collection_time        NUMBER(20),
  span_young_gc_collection_count NUMBER(20),
  span_young_gc_collection_time  NUMBER(20),
  span_full_gc_collection_count  NUMBER(20),
  span_full_gc_collection_time   NUMBER(20)
)