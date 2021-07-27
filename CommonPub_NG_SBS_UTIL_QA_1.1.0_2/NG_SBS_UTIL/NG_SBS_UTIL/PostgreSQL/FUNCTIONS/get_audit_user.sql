CREATE OR REPLACE FUNCTION sbs_util.get_audit_user()
RETURNS CHARACTER VARYING
LANGUAGE 'plpgsql'    
AS $BODY$
DECLARE
 --------------------------------------------------------------------------
	-- Purpose : return user who initiated the process.
      -----------------------------------------------------------------------------------------------------------------------------
   --
   --------------------------------------------------------------------------------
   -- Name: get_audit_user
   --------------------------------------------------------------------------------
   --
   -- Description:  get_audit_user
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Sakshi Jain CSPUBCC-4307	04/06/2021	[NextGen] SBS_UTIL Feature for get_audit, get_base, get_hash 
    l_audituser 		VARCHAR(63);
	l_format_call_stack TEXT;
  BEGIN
    -- Retrieve Schema Name/USER who initiated the Process,OS_USER cannot be Retrieved.
    l_audituser := COALESCE(CURRENT_USER,SESSION_USER);

    RETURN l_audituser;
EXCEPTION
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in function :- get_audit_user(), ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;