CREATE OR REPLACE PROCEDURE sbs_util.release_lock
(
	in_lock_name		VARCHAR(200)
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   -------------------------------------------------------------------------------------
   -- Name : RELEASE_LOCK
   -------------------------------------------------------------------------------------
   -- Description : This procedure is to release a Lock - ExclusiveLock or ShareLock which was accquired.
   --
   --Input Parameter details
   --in_lock_name		- Name of the Lock that needs to be added.
   
   -----------------------------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ------------------------------------------------------------------
   -- 	1	Kalyan	     CSPUBCC-4324	06/29/2021	release_lock() initial draft
   -------------------------------------------------------------------------------------
	l_key_value 			BIGINT;
	l_lock_name				VARCHAR(200);
	l_lock_mode				VARCHAR(100);
	l_lock_exists			SMALLINT;
	l_status				BOOLEAN;
	l_format_call_stack    	TEXT;
	CO_EXCLUSIVE_LOCK		VARCHAR(20) := 'EXCLUSIVELOCK';
	CO_SHARED_LOCK			VARCHAR(20) := 'SHARELOCK';

BEGIN
	
	l_lock_name := TRIM(in_lock_name);
	
	-- Validation for Lock Name.
	IF (l_lock_name IS NULL OR l_lock_name = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	END IF;
	
	-- Calling of allocate_unique_lockhandle function to get 64 bit unique number generated for that Lock Name, 
	-- which will be passed to PG Lock methods.
	-- in_lock_request defines that this method is called from release_lock procedure.
	l_key_value := sbs_util.allocate_unique_lockhandle(in_lock_name => l_lock_name, in_lock_request => 'RELEASE_LOCK');
	RAISE INFO 'l_key_value - %', l_key_value;
	
	-- To check if Lock with that name exists. If exists, 
	-- then only PG unlock methods will be called.
	SELECT COUNT(1) INTO l_lock_exists FROM pg_locks WHERE locktype = 'advisory'
	AND (classid::bigint<<32|objid::bigint) = l_key_value;
	
	IF l_lock_exists > 0 THEN
		
		-- If Lock with the name passed exists, it will fetch the MODE of that Lock.
		SELECT UPPER(MODE::VARCHAR) INTO l_lock_mode FROM pg_locks WHERE locktype = 'advisory'
		AND (classid::BIGINT<<32|objid::BIGINT) = l_key_value;
		
		-- If lock mode = EXCLUSIVELOCK, it will call pg_advisory_lock method.
		IF l_lock_mode = CO_EXCLUSIVE_LOCK THEN
			RAISE INFO 'Lock with this name exists.';		
			SELECT INTO l_status pg_advisory_unlock(l_key_value);

		-- If lock mode = SHARELOCK, it will call pg_advisory_lock method.
		ELSIF l_lock_mode = CO_SHARED_LOCK THEN
			RAISE INFO 'Lock with this name exists.';		
			SELECT INTO l_status pg_advisory_unlock_shared(l_key_value);
		END IF;
	
		IF l_status = TRUE THEN
			RAISE INFO 'Lock has been released';
		ELSIF l_status = FALSE THEN
			RAISE INFO 'Lock has not been released';
		END IF;
		
	-- When Lock with that name does not exists, it will raise error.
	ELSIF l_lock_exists = 0 THEN
		RAISE INFO 'There is no lock held with this name.';
		RAISE EXCEPTION USING ERRCODE = 50002;
	END IF;
	
EXCEPTION
	WHEN null_value_not_allowed THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- release_lock(), input parameter in_lock_name / in_lock_mode cannot be null or empty' , CHR(10), 'SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- release_lock(), Please provide correct Lock Name. Lock name that was passed is - ', l_lock_name , CHR(10), 'SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- release_lock()', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
