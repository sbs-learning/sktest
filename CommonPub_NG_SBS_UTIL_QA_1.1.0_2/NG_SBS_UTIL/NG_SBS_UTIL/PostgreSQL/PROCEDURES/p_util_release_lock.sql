CREATE OR REPLACE PROCEDURE sbs_util.p_util_release_lock
(
	in_lock_name		VARCHAR(200)
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   -------------------------------------------------------------------------------------
   -- Name : P_UTIL_RELEASE_LOCK
   -------------------------------------------------------------------------------------
   -- Description : This is wrapper funtion to RELEASE_LOCK procedure.
   --
   --Input Parameter details
   --in_lock_name		- Name of the Lock that needs to be added.
   
   -----------------------------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ------------------------------------------------------------------
   -- 	1	Kalyan	     CSPUBCC-4324	06/29/2021	p_util_release_lock() initial draft
   -------------------------------------------------------------------------------------
	l_lock_name				VARCHAR(200);
	l_format_call_stack    	TEXT;
BEGIN
	
	l_lock_name := TRIM(in_lock_name);
	
	-- Validation for Lock Name.
	IF (l_lock_name IS NULL OR l_lock_name = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	END IF;
	
	CALL sbs_util.RELEASE_LOCK
		(
			in_lock_name         => l_lock_name
		);
		
EXCEPTION
	WHEN null_value_not_allowed THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- p_util_release_lock(), input parameter in_lock_name / in_lock_mode cannot be null or empty' , CHR(10), 'SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- p_util_release_lock()', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
