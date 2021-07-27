CREATE OR REPLACE FUNCTION sbs_util.get_bl_user_schema()
RETURNS VARCHAR
LANGUAGE 'plpgsql'
AS
$BODY$
    --------------------------------------------------------------------------------
    -- Name: GET_BL_USER_SCHEMA
    --------------------------------------------------------------------------------
    --
    -- Description:  Returns the name of the BASELINE_USER schema on basis of role.
    --
    --------------------------------------------------------------------------------
	  -- RefNo Name            JIRA NO 		Date     	Description of change
      -- ----- ---------------- -------- ---------------------------------------------
      -- 	1	Sakshi	     CSPUBCC-4397	04/08/2021	get schema name initial draft
  DECLARE
	l_format_call_stack text;
  BEGIN
    RETURN 'bl_user';
EXCEPTION
  WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
			CALL sbs_util.error_handler(
					in_error_stack => l_format_call_stack, 
					in_error_code => SQLSTATE, 
					in_error_message => ' Error in function :- get_bl_user_schema() ' || chr(10) || 'SQLERRM: ' || sqlerrm, 
					in_show_stack => TRUE
				);
  END;
$BODY$;