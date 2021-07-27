CREATE OR REPLACE PROCEDURE sbs_util.request_lock
(
	in_lock_name		VARCHAR(200),
	in_lock_mode 		VARCHAR(100),
	in_wait_min 		INTEGER
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   -------------------------------------------------------------------------------------
   -- Name : REQUEST_LOCK
   -------------------------------------------------------------------------------------
   -- Description : This procedure is to Request for a Lock - ExclusiveLock or ShareLock for the current working session.
   --
   --Input Parameter details
   --in_lock_name		- Name of the Lock that needs to be added.
   --in_lock_mode		- Mode of the Lock that needs accquired.
   --in_wait_min		- Time in Minutes to wait for a lock if that is already acquired
   
   -----------------------------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ------------------------------------------------------------------
   -- 	1	Kalyan	     CSPUBCC-4324	06/29/2021	request_lock() initial draft
   -------------------------------------------------------------------------------------

	l_key_value 			BIGINT;
	l_lock_name				VARCHAR(200);
	l_lock_mode				VARCHAR(100);
	l_lock_exists			SMALLINT;
	l_sql					TEXT;
	l_format_call_stack    	TEXT;
	l_current_time			TIMESTAMP;
	l_out_time				TIMESTAMP;
	l_status				VARCHAR;
	CO_EXCLUSIVE_LOCK		VARCHAR(20) := 'EXCLUSIVELOCK';
	CO_SHARED_LOCK			VARCHAR(20) := 'SHARELOCK';
BEGIN
	-- 30 seconds of sleep time for the process to avoid any concurrent process.
	PERFORM pg_sleep(30);
	
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
	
	-- Value for in_wait_min should be greater than 0, if not it will raise error.
	IF in_wait_min < 0 THEN
		RAISE EXCEPTION USING ERRCODE = 50002;
	END IF;
	
	-- Value for l_lock_mode should be EXCLUSIVELOCK or SHARELOCK, if not it will raise error.
	IF (UPPER(l_lock_mode) NOT IN (CO_EXCLUSIVE_LOCK, CO_SHARED_LOCK)) THEN
		RAISE EXCEPTION USING ERRCODE = 50003;
    END IF;
	
	-- Calling of allocate_unique_lockhandle function to get 64 bit unique number generated for that Lock Name, 
	-- which will be passed to PG Lock methods. 
	-- in_lock_request defines that this method is called from Request_lock procedure.
	l_key_value := sbs_util.allocate_unique_lockhandle(in_lock_name => l_lock_name, in_lock_request => 'REQUEST_LOCK');
	RAISE INFO 'l_key_value - %', l_key_value;
	
	-- To check if Lock with that name already exists. If exists, 
	-- the new request will wait for the time that has been mentioned in in_wait_min parameter.
	SELECT COUNT(1) INTO l_lock_exists FROM pg_locks WHERE LOWER(locktype) = 'advisory' 
	AND (classid::BIGINT<<32|objid::BIGINT) = l_key_value;
	
	-- When Lock does not exists.
	IF l_lock_exists = 0 THEN
		
		-- When Lock is Exclusive Lock, it will call pg_advisory_lock method.
		IF l_lock_mode = CO_EXCLUSIVE_LOCK THEN
			RAISE INFO 'Lock Does not Exists';
			RAISE INFO 'Performing Lock';
			SELECT INTO l_status pg_advisory_lock(l_key_value);
			RAISE INFO 'Lock Enabled';
			
		-- When Lock is Share Lock, it will call pg_advisory_lock_shared method.
		ELSIF l_lock_mode = CO_SHARED_LOCK THEN
			RAISE INFO 'Lock Does not Exists';
			RAISE INFO 'Performing Lock';
			SELECT INTO l_status pg_advisory_lock_shared(l_key_value);
			RAISE INFO 'Lock Enabled';
		END IF;
		
	-- When Lock with that name already exists.
	ELSIF l_lock_exists > 0 THEN
		RAISE INFO 'Lock Already Exists';
		
		-- Variable to hold the current time 
		l_current_time := clock_timestamp();
		
		-- Query to find clock time when process started to wait till the value provided in in_wait_min parameter.
		-- current_time + Interval
		-- This wil get stored in l_out_time variable.
		l_sql := 'select '''||l_current_time||'''::TIMESTAMPTZ + INTERVAL '''||in_wait_min || ' min ''' ;
		EXECUTE l_sql INTO l_out_time;
		
		RAISE INFO 'l_current_time - %',l_current_time;
		RAISE INFO 'l_out_time - %',l_out_time;
		
		-- Condition for the process to wait and check if Lock got is available till the out time reaches.
		WHILE l_current_time::TIMESTAMP <= l_out_time::TIMESTAMP LOOP
			
			-- To check if Lock with that name is still acquired by another process. If yes, 
			-- the new request will wait for a interval of 5 sec and then again check for Lock availability.
			SELECT COUNT(1) INTO l_lock_exists FROM pg_locks WHERE LOWER(locktype) = 'advisory' 
			AND (classid::bigint<<32|objid::bigint) = l_key_value;
			
			-- Condition to check if lock held from old session still exists.
			IF l_lock_exists > 0 THEN
					RAISE INFO 'Inside IF condition of WHILE Loop';
				-- Perform a wait of 5 Sec.
				PERFORM pg_sleep(10);
				-- Reintialize the clock_timestamp value to current time and check if Loop condition is still valid.
				SELECT clock_timestamp() INTO l_current_time;
					RAISE INFO 'l_current_time reintitialised';
					RAISE INFO 'l_current_time - %',l_current_time;
				
				-- The new clock_timestamp is then checked with out time value, 
				-- If value of current time exceeds out_time it will raise error that Max wait time has reached and no Lock was accquired.
				IF l_current_time::TIMESTAMP >= l_out_time::TIMESTAMP THEN
					RAISE INFO 'Max wait time completed, no lock was obtained';
					RAISE EXCEPTION USING ERRCODE = 50004;
				END IF;
			-- If old Lock is released, the count in l_lock_exists will come to 0.
			ELSIF l_lock_exists = 0 THEN
				
				-- When Lock is Exclusive Lock, it will call pg_advisory_lock method for the new session.
				IF l_lock_mode = CO_EXCLUSIVE_LOCK THEN
					RAISE INFO 'Inside ELSIF condition of WHILE Loop';
					RAISE INFO 'Performing Lock';
					SELECT INTO l_status pg_advisory_lock(l_key_value);
					RAISE INFO 'Lock Enabled';
					EXIT;
					
				-- When Lock is Share Lock, it will call pg_advisory_lock_shared method for the new session.
				ELSIF l_lock_mode = CO_SHARED_LOCK THEN
					RAISE INFO 'Inside ELSIF condition of WHILE Loop';
					RAISE INFO 'Performing Lock';
					SELECT INTO l_status pg_advisory_lock_shared(l_key_value);
					RAISE INFO 'Lock Enabled';
					EXIT;
				END IF;
			END IF;
		END LOOP;
	END IF;
EXCEPTION
	WHEN null_value_not_allowed THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- request_lock(), input parameter in_lock_name / in_lock_mode / in_wait_min cannot be null or empty' , CHR(10), 'SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- request_lock(), The value of input parameter in_wait_min time in min should be greater than 0.' , CHR(10), 'SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- request_lock(), The value of input parameter in_lock_mode should have values EXCLUSIVELOCK or SHARELOCK.' , CHR(10), 'SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);			
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- request_lock(), Max wait time completed, no lock was obtained' , CHR(10), 'SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in procedure :- request_lock()', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
