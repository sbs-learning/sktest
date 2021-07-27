CREATE OR REPLACE FUNCTION sbs_util.get_lt_schema()
RETURNS VARCHAR
LANGUAGE 'plpgsql'
AS
$BODY$
    --------------------------------------------------------------------------------
    -- Name: GET_LT_SCHEMA
    --------------------------------------------------------------------------------
    --
    -- Description:  Returns the name of the LT schema on basis of role.
    --
    --------------------------------------------------------------------------------
	  -- RefNo Name            JIRA NO 		Date     	Description of change
      -- ----- ---------------- -------- ---------------------------------------------
      -- 	1	Kalyan	     CSPUBCC-4484	05/07/2021	get schema name initial draft
DECLARE
	l_format_call_stack text;
BEGIN
	RETURN 'lt';
EXCEPTION
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_lt_schema() ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;