CREATE OR REPLACE FUNCTION sbs_util.get_schema_owner
(
	in_related_schema VARCHAR(200)
)
RETURNS VARCHAR
LANGUAGE 'plpgsql'
AS
$BODY$
    --------------------------------------------------------------------------------
    -- Name: GET_SCHEMA_OWNER
    --------------------------------------------------------------------------------
    --
    -- Description: Returns owner of that schema name passed in parameter.
    --
    --------------------------------------------------------------------------------
	  -- RefNo Name            JIRA NO 		Date     	Description of change
      -- ----- ---------------- -------- ---------------------------------------------
      -- 	1	Kalyan	     CSPUBCC-4484	07/05/2021	get schema owne initial draft
DECLARE
	l_schema_owner		VARCHAR(100);
	l_sql				VARCHAR(2000);
	l_related_schema	VARCHAR(200);
	l_format_call_stack TEXT;
BEGIN
	l_related_schema := LOWER(in_related_schema);
	
	IF (l_related_schema IS NULL OR l_related_schema = '') THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;

	l_sql := 'SELECT u.usename
		        FROM pg_catalog.pg_namespace s
		        JOIN pg_catalog.pg_user u on u.usesysid = s.nspowner
		       WHERE s.nspname = '''||l_related_schema||''' ';
	
	EXECUTE l_sql INTO STRICT l_schema_owner;

	RETURN l_schema_owner;

EXCEPTION
	WHEN NO_DATA_FOUND THEN  
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- get_schema_owner(), parameter in_related_schema passed is incorrect. No user found as owner of this schema. Schema name passed is :- ', in_related_schema, CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50001' THEN  
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- get_schema_owner(), Input parameter in_related_schema is NULL or empty', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack, 
				in_error_code 		=> SQLSTATE, 
				in_error_message 	=> CONCAT(' Error in function :- get_schema_owner() ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack 		=> TRUE
			);
  END;
$BODY$;
