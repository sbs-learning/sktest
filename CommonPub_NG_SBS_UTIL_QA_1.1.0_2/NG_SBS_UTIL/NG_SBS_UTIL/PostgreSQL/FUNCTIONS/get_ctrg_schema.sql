CREATE OR REPLACE FUNCTION sbs_util.get_ctrg_schema(
	)
    RETURNS character varying
    LANGUAGE 'plpgsql'
AS $BODY$
--------------------------------------------------------------------------------
    -- Name: GET_CTRG_SCHEMA
    --------------------------------------------------------------------------------
    --
    -- Description:  Returns the name of the ctrg schema on basis of role.
    --
    --------------------------------------------------------------------------------
	  -- RefNo Name            JIRA NO 		Date     	Description of change
      -- ----- ---------------- -------- ---------------------------------------------
      -- 	1	Sakshi	     CSPUBCC-4397	04/08/2021	get schema name initial draft
  DECLARE
	l_role varchar(100);
	l_format_call_stack text;
  BEGIN
    RETURN 'ctrg';
EXCEPTION
  WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
			CALL sbs_util.error_handler(
					in_error_stack => l_format_call_stack, 
					in_error_code => SQLSTATE, 
					in_error_message => ' Error in function :- get_ctrg_schema() ' || chr(10) || 'SQLERRM: ' || sqlerrm, 
					in_show_stack => TRUE
				);
  END;
$BODY$;