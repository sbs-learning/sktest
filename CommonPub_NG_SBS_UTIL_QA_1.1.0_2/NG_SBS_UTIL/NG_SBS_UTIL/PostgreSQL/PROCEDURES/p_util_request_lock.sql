CREATE OR REPLACE PROCEDURE sbs_util.p_util_request_lock
(
	in_lock_name		VARCHAR(200),
	in_lock_mode 		VARCHAR(100),
	in_wait_min 		INTEGER
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   -------------------------------------------------------------------------------------
   -- Name : P_UTIL_REQUEST_LOCK
   -------------------------------------------------------------------------------------
   -- Description :  This is wrapper funtion to REQUEST_LOCK procedure.
   --
   --Input Parameter details
   --in_lock_name		- Name of the Lock that needs to be added.
   --in_lock_mode		- Mode of the Lock that needs accquired.
   --in_wait_min		- Time in Minutes to wait for a lock if that is already acquired
   
   -----------------------------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ------------------------------------------------------------------
   -- 	1	Kalyan	     CSPUBCC-4324	06/29/2021	p_util_request_lock() initial draft
   -------------------------------------------------------------------------------------

	l_lock_name				VARCHAR(200);
	l_lock_mode				VARCHAR(100);
	l_format_call_stack    	TEXT;
	CO_EXCLUSIVE_LOCK		VARCHAR(20) := 'EXCLUSIVELOCK';
	CO_SHARED_LOCK			VARCHAR(20) := 'SHARELOCK';
BEGIN
	
	l_lock_name := TRIM(in_lock_name);
	l_lock_mode := UPPER(TRIM(in_lock_mode));
	
	-- Validation for Lock Name, Lock Mode and Wait Min time.
	IF (l_lock_name IS NULL OR l_lock_name = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
    ELSIF (l_lock_mode IS NULL OR l_lock_mode = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
    ELSIF (in_wait_min IS NULL) THEN
		RAISE EXCEPTION null_value_not_allowed;
	END IF;
	
	-- Value for in_wait_min should be greater than 1, if not it will raise error.
	IF in_wait_min < 0 THEN
		RAISE EXCEPTION USING ERRCODE = 50002;
	END IF;
	
	-- Value for l_lock_mode should be EXCLUSIVELOCK or SHARELOCK, if not it will raise error.
	IF (UPPER(l_lock_mode) NOT IN (CO_EXCLUSIVE_LOCK, CO_SHARED_LOCK)) THEN
		RAISE EXCEPTION USING ERRCODE = 50003;
    END IF;
	
	CALL sbs_util.REQUEST_LOCK
		(
			in_lock_name		=> l_lock_name,
			in_lock_mode 		=> l_lock_mode,
			in_wait_min			=> in_wait_min
		);
	
EXCEPTION
	WHEN null_value_not_allowed THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- p_util_request_lock(), input parameter in_lock_name / in_lock_mode / in_wait_min cannot be null or empty' , CHR(10), 'SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- p_util_request_lock(), The value of input parameter in_wait_min time in min should be greater than 0.' , CHR(10), 'SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- p_util_request_lock(), The value of input parameter in_lock_mode should have values EXCLUSIVELOCK or SHARELOCK.' , CHR(10), 'SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);			
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in procedure :- p_util_request_lock()', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;