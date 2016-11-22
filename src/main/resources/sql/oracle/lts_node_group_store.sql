create table {tableName}
(
  node_type   NVARCHAR2(16) not null,
  name        NVARCHAR2(64) not null,
  gmt_created NUMBER(20),
  primary key (node_type, name)
)