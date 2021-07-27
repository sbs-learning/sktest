CREATE OR REPLACE FUNCTION sbs_util.allocate_unique_lockhandle
(
	in_lock_name	VARCHAR(200),
	in_lock_request VARCHAR(20)
)
RETURNS bigint
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   -------------------------------------------------------------------------------------
   -- Name : ALLOCATE_UNIQUE_LOCKHANDLE
   -------------------------------------------------------------------------------------
   -- Description : This function is to generate a unique 64 digit Number for the Lock name that is passed.
   -- 				Thsi s done because the PG lock methods accepts a numeric value(bigint) as parameter and Lock is held or released.
   --
   --Input Parameter details
   --in_lock_name		- Name of the Lock that needs to be added.
   --in_lock_request	- The value contains where t his process is being called from, either from Request Lock or Release Lock.
   -----------------------------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ------------------------------------------------------------------
   -- 	1	Kalyan	     CSPUBCC-4324	06/29/2021	allocate_unique_lockhandle() initial draft
   -------------------------------------------------------------------------------------
   
	l_lock_name_exists		SMALLINT;
	l_key_value				BIGINT;
	l_lock_request			VARCHAR(20);
	l_lock_name				VARCHAR(200);
	l_sql					TEXT;
	l_format_call_stack    	TEXT;
	
BEGIN
	l_lock_name := TRIM(in_lock_name);
	l_lock_request := TRIM(in_lock_request);
	
	-- Validation for Lock Name.
	IF (l_lock_name IS NULL OR l_lock_name = '') THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
    END IF;
	-- Query to check if the Lock name that is passed is already present in sbs_pg_session_lock table, treating this as Metadata for all Locks.
	-- If already present, then this will not do any Insert into this table.
	SELECT COUNT(1) INTO l_lock_name_exists FROM sbs_util.sbs_pg_session_lock
	WHERE lock_name = l_lock_name;
	
	-- If Lock Name already exists, it will only return 64 bit value for that lock name.
	IF l_lock_name_exists > 0 THEN
		SELECT lock_key INTO l_key_value FROM sbs_util.sbs_pg_session_lock
		WHERE lock_name = l_lock_name;

	-- If Lock Name does not exists, and this function is being called from Request Lock method, 
	-- Then it will do an insert into the metadata table.
	-- if this method is being called from release Lock it will not do anything and the return value will be NULL.
	
	ELSIF l_lock_name_exists = 0 AND l_lock_request = 'REQUEST_LOCK' THEN
		--l_key_value := sbs_util.h_bigint(l_lock_name);
		l_key_value := ('x'||substr(md5(l_lock_name),1,16))::bit(64)::bigint;
		
		-- Query to Insert into sbs_pg_session_lock table with Lock name and 64 bit l_key_value.
		l_sql :=
		'INSERT INTO sbs_util.sbs_pg_session_lock (lock_name, lock_key)
		VALUES( '''||l_lock_name||''', '''||l_key_value||''')
		ON CONFLICT(lock_name)
		DO NOTHING';
		RAISE INFO 'l_sql - %', l_sql;
		EXECUTE l_sql;
	END IF;
	RETURN l_key_value;
EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in function :- allocate_unique_lockhandle(), input parameter in_lock_name cannot be null or empty' , CHR(10), 'SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in function :- allocate_unique_lockhandle()', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;