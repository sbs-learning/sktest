CREATE OR REPLACE FUNCTION sbs_util.get_pub_work_schema()
RETURNS VARCHAR
LANGUAGE 'plpgsql'
AS
$BODY$
    --------------------------------------------------------------------------------
    -- Name: GET_PUB_WORK_SCHEMA
    --------------------------------------------------------------------------------
    --
    -- Description:  Returns the name of the PUB_WORK schema on basis of role.
    --
    --------------------------------------------------------------------------------
	  -- RefNo Name            JIRA NO 		Date     	Description of change
      -- ----- ---------------- -------- ---------------------------------------------
      -- 	1	Sakshi	     CSPUBCC-4397	04/08/2021	get schema name initial draft
  DECLARE
	l_format_call_stack text;
  BEGIN
    RETURN 'pub_work';
EXCEPTION
  WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
			CALL sbs_util.error_handler(
					in_error_stack => l_format_call_stack, 
					in_error_code => SQLSTATE, 
					in_error_message => ' Error in function :- get_pub_work_schema() ' || chr(10) || 'SQLERRM: ' || sqlerrm, 
					in_show_stack => TRUE
				);
  END;
$BODY$;