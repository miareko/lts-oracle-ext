CREATE OR REPLACE FUNCTION FUNC_IFTABLEEXISTS(tableName IN VARCHAR2)
RETURN NUMBER
AS
   v_count NUMBER;
BEGIN
  SELECT COUNT(1) INTO v_count FROM user_tables WHERE table_name = UPPER(tableName);
  RETURN v_count;
END;