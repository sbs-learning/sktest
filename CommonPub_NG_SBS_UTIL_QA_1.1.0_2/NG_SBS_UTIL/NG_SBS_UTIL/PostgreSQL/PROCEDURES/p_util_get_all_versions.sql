CREATE OR REPLACE PROCEDURE sbs_util.p_util_get_all_versions()
LANGUAGE 'plpgsql'
SECURITY DEFINER
AS $BODY$
DECLARE
   /* --------------------------------------------------------------------------
   --
   --------------------------------------------------------------------------------
   -- Name: p_util_get_all_versions
   --------------------------------------------------------------------------------
   --
   -- Description:  Purpose of this procedure is to get the versions of all tools from their version table
   --               from all schemas of that stack.
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Kalyan	     CSPUBCC-4401	04/20/2021	p_util_get_all_versions() initial draft
   */

	l_format_call_stack TEXT;
BEGIN
	CALL sbs_util.get_all_versions ();
EXCEPTION
    WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in Procedure :- p_util_get_all_versions(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;