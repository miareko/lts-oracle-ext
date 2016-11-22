CREATE OR REPLACE PROCEDURE PROC_DROPIFEXISTS ( tableName IN VARCHAR2 )
IS
v_count NUMBER(10);
BEGIN
      
   select count(*)
   into v_count
   from user_tables
   where table_name = upper(tableName);

   if v_count > 0 then
      execute immediate 'drop table ' || tableName;
   end if;
END proc_dropifexists;