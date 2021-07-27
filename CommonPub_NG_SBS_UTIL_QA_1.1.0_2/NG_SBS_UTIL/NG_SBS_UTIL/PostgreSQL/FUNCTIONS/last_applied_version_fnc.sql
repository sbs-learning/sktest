CREATE OR REPLACE FUNCTION sbs_util.last_applied_version_fnc
(
	in_version_table CHARACTER VARYING
)
RETURNS CHARACTER VARYING
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
      -------------------------------------------------------------------------------------
      -- Name : last_applied_version_fnc
      -------------------------------------------------------------------------------------
      -- Description : Returns the most-recently applied version
      --
      -- Logic : Query  Version Table and return value
      --
      --Input Parameter details
      --IN_VERSION_TABLE  - Version table of the tool
	  --------------------------------------------------------------------------------
      -- RefNo Name            JIRA NO 		Date     Description of change
      -- ----- ---------------- -------- ---------------------------------------------
      -- 	1	Akshay	     CSPUBCC-4301	04/08/2021	last_applied_version_fnc() initial draft
      -- 	2	Akshay	     CSPUBCC-4383	04/15/2021	Added variable l_version_table
      --                                               and lower function in query
      -------------------------------------------------------------------------------------
      l_last_applied_version    VARCHAR;
      l_sql_txt                 VARCHAR(32767);
	  l_version_table           VARCHAR(100);
	  l_format_call_stack       Text;
BEGIN
     l_version_table := LOWER(TRIM(in_version_table));

    IF (l_version_table IS NULL OR l_version_table = '') THEN
		RAISE EXCEPTION USING errcode = 50001;
    END IF;
	
    L_SQL_TXT:='SELECT version FROM '||l_version_table||' WHERE IS_CURRENT=1';
	
    EXECUTE L_SQL_TXT INTO STRICT l_last_applied_version;
	
    RETURN l_last_applied_version;
	
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;	
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,		
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in function :- last_applied_version_fnc(), input parameter in_version_table not accept null or empty value. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in function :- last_applied_version_fnc(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;