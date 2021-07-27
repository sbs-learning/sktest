CREATE OR REPLACE PROCEDURE p_util_gather_table_n_partition_stats
(
	in_schema_nm 				CHARACTER VARYING,
	in_table_nm 				CHARACTER VARYING,
	in_partition_nm 			CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING,
	in_partition_val 			CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING,
	in_subpartition_val 		CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING,
	in_user_id 					CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING,
	in_gather_stale_only_flg 	BOOLEAN DEFAULT FALSE
)
LANGUAGE 'plpgsql'
    SECURITY DEFINER
AS $BODY$
DECLARE

	l_format_call_stack 	TEXT;
	l_crsr_partition_nm 	RECORD;
	l_param_count 			SMALLINT;
	l_max_attempts 			VARCHAR(100) 	:= NULL;
	l_wait_time_sec 		VARCHAR(100) 	:= NULL;
	l_attempts 				SMALLINT 		:= 0;
	l_schema_nm 			VARCHAR(150);
	l_table_nm 				VARCHAR(150);
	l_partition_nm 			VARCHAR(150);
	l_partition_val 		VARCHAR(150);
	l_user_id           	VARCHAR(100);
	l_table_exists    		VARCHAR(1);
BEGIN

	l_schema_nm 			:= LOWER(TRIM(in_schema_nm));
	l_table_nm 				:= LOWER(TRIM(in_table_nm));
	l_partition_nm 			:= LOWER(TRIM(in_partition_nm));
	l_partition_val 		:= LOWER(TRIM(in_partition_val));
	l_user_id 				:= COALESCE(in_user_id, sbs_util.get_audit_user());
	
	if(l_schema_nm = '' ) then
		RAISE EXCEPTION USING ERRCODE = 50005;
	END IF;
	if(l_table_nm = '') then
		RAISE EXCEPTION USING ERRCODE = 50006;
	END IF;
	
	if(l_partition_nm = '' or  l_partition_nm is NULL) and (l_partition_val is NULL or l_partition_val = '') then
		RAISE EXCEPTION USING ERRCODE = 50007;
	END IF;
	
	if(l_partition_val = '' and  l_partition_nm is NOT NULL) or (l_partition_val is NOT NULL and l_partition_nm = '') then
		RAISE EXCEPTION USING ERRCODE = 50008;
	END IF;
	
	IF (l_partition_nm IS NOT NULL AND l_partition_val IS NOT NULL)
	THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
    END IF;

	IF in_gather_stale_only_flg IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50009;
	END IF;

	SELECT COUNT(1) INTO l_param_count
	  FROM pub_admin.pba_param_t
	 WHERE UPPER(param_nm) IN ('GTS_MAX_ATTEMPTS','GTS_ATTEMPTS_WAIT_TIME_SEC');

	IF l_param_count = 2
	THEN
		
		l_max_attempts  := TRIM(pub_admin.get_parameter('GTS_MAX_ATTEMPTS'));
        l_wait_time_sec := TRIM(pub_admin.get_parameter('GTS_ATTEMPTS_WAIT_TIME_SEC'));

		-- to check if the value provided contains any invalid character or not, if provided raise error. valid values are any numeric value.
		IF ((l_max_attempts ~ '^[0-9]*$') AND (l_wait_time_sec ~ '^[0-9]*$')) THEN
			RAISE INFO 'valid value';
		ELSE
			-- raise error if invalid value is encountered.
            RAISE INFO 'invalid value in pba_param_t for GTS_MAX_ATTEMPTS, GTS_ATTEMPTS_WAIT_TIME_SEC parameter';
            --RAISE EXCEPTION 'invalid value in pbl_param_t for GTS_MAX_ATTEMPTS, GTS_ATTEMPTS_WAIT_TIME_SEC parameter';
			RAISE EXCEPTION USING ERRCODE = 50002;
		END IF;
	ELSIF l_param_count = 1 
	THEN
			-- raise error if count is 1, means any one row is missing in PBL_PARAM_T Table for these parameters.
		--RAISE EXCEPTION 'value in pba_param_t not set for either of one GTS_MAX_ATTEMPTS, GTS_ATTEMPTS_WAIT_TIME_SEC parameter';
		RAISE EXCEPTION USING ERRCODE = 50003;
	ELSE
        RAISE INFO 'No Data found in PBL_PARAM_T Table for GTS_MAX_ATTEMPTS, GTS_ATTEMPTS_WAIT_TIME_SEC parameters, Setting values to default';
		l_max_attempts := 5;
        l_wait_time_sec := 60;
	END IF;       

	RAISE INFO 'l_max_attempts :%',l_max_attempts;
	RAISE INFO 'l_wait_time_sec :%',l_wait_time_sec;

	IF l_schema_nm IS NOT NULL AND l_table_nm IS NOT NULL THEN
        BEGIN
            SELECT 'Y'
              INTO STRICT l_table_exists
              FROM information_schema.tables
             WHERE table_schema = l_schema_nm
			   AND TRIM(table_name,'"') = l_table_nm;

		EXCEPTION
			WHEN NO_DATA_FOUND THEN
				RAISE EXCEPTION USING ERRCODE = 50004;
		END;
	ELSIF l_schema_nm IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50005;
    ELSIF l_table_nm IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50006;
    END IF;

	IF (l_partition_nm IS NOT NULL) THEN
		FOR l_crsr_partition_nm IN (SELECT regexp_split_to_table(in_partition_nm, ',') AS partition_name)
		LOOP
			LOOP
				l_attempts := l_attempts + 1;
				RAISE INFO '%', l_attempts;
				BEGIN
					CALL pub_admin.handle_table_n_partition_stats
						(
							in_schema_nm    			=> l_schema_nm,
							in_table_nm     			=> l_table_nm,
							in_partition_nm 			=> TRIM(l_crsr_partition_nm.partition_name),
							in_partition_val 			=> l_partition_val,
							in_subpartition_val 		=> in_subpartition_val,
							in_user_id      			=> l_user_id,
							in_gather_stale_only_flg 	=> in_gather_stale_only_flg
						);
					EXIT;    -- Exit when Success
				EXCEPTION WHEN OTHERS THEN
					IF l_attempts >= l_max_attempts::NUMERIC THEN
						GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
							CALL sbs_util.error_handler
								(
									in_error_stack => l_format_call_stack,
									in_error_code => SQLSTATE,
									in_error_message => CONCAT(' Error in Procedure :- p_util_gather_table_n_partition_stats(), ',  ' Maximum attempts completed for gather table partition stats ' ,  CHR(10), ' SQLERRM :- ', SQLERRM),
									in_show_stack => TRUE
								);
					ELSE
						PERFORM pg_sleep(l_wait_time_sec::NUMERIC);
					END IF;
				END;
			END LOOP;
		END LOOP;
	ELSIF(in_partition_val IS NOT NULL) THEN
		FOR l_crsr_partition_nm IN (SELECT regexp_split_to_table(in_partition_val, ',') AS partition_value)
		Loop
			LOOP
				l_attempts := l_attempts + 1;
				RAISE INFO '%', l_attempts;
				BEGIN
					CALL pub_admin.handle_table_n_partition_stats
						(
							in_schema_nm    			=> l_schema_nm,
							in_table_nm     			=> l_table_nm,
							in_partition_nm 			=> l_partition_nm,
							in_partition_val 			=> l_crsr_partition_nm.partition_value,
							in_subpartition_val 		=> in_subpartition_val,
							in_user_id      			=> l_user_id,
							in_gather_stale_only_flg 	=> in_gather_stale_only_flg
						);
					EXIT;    -- Exit when Success
				EXCEPTION WHEN OTHERS THEN
					IF l_attempts >= l_max_attempts::NUMERIC THEN
						GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
						CALL sbs_util.error_handler
							(
								in_error_stack => l_format_call_stack,
								in_error_code => SQLSTATE,
								in_error_message => CONCAT(' Error in Procedure :- p_util_gather_table_n_partition_stats(), ',  ' Maximum attempts completed for gather table partition stats ' , CHR(10), ' SQLERRM :- ',  SQLERRM),
								in_show_stack => TRUE
							);
					ELSE
						PERFORM pg_sleep(l_wait_time_sec::NUMERIC);   
					END IF;
				END;
			END LOOP;
		END LOOP;
	ELSE
		LOOP
			l_attempts := l_attempts + 1;
			RAISE INFO '%', l_attempts;
			BEGIN
				CALL pub_admin.handle_table_n_partition_stats
					(
						in_schema_nm    			=> l_schema_nm,
						in_table_nm     			=> l_table_nm,
						in_partition_nm 			=> l_partition_nm,
						in_partition_val 			=> l_partition_val,
						in_subpartition_val 		=> in_subpartition_val,
						in_user_id      			=> l_user_id,
						in_gather_stale_only_flg 	=> in_gather_stale_only_flg
					);
				EXIT;    -- Exit when Success
			EXCEPTION WHEN OTHERS THEN
				IF l_attempts >= l_max_attempts::NUMERIC THEN
					GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
					CALL sbs_util.error_handler
						(
							in_error_stack => l_format_call_stack,
							in_error_code => SQLSTATE,
							in_error_message => CONCAT(' Error in Procedure :- p_util_gather_table_n_partition_stats(), ',  ' Maximum attempts completed for gather table partition stats ' , CHR(10), ' SQLERRM :- ',  SQLERRM),
							in_show_stack => TRUE
						);
				ELSE
					PERFORM pg_sleep(l_wait_time_sec::NUMERIC);
				END IF;
			END;
		END LOOP;
	END IF;
EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_gather_table_n_partition_stats(), Analyze a partition either by name or by value. ', CHR(10), ' SQLERRM :-  ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_gather_table_n_partition_stats(), invalid value in pba_param_t for GTS_MAX_ATTEMPTS, GTS_ATTEMPTS_WAIT_TIME_SEC parameter. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_gather_table_n_partition_stats(), value in pba_param_t not set for either of one GTS_MAX_ATTEMPTS, GTS_ATTEMPTS_WAIT_TIME_SEC parameter ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_gather_table_n_partition_stats(), Schema name or Table name does not exists.  Schema name passed - ',in_schema_nm, ', Table name passed - ',in_table_nm, CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50005' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_gather_table_n_partition_stats(), input parameter passed for in_schema_nm cannot have null or empty value.', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50006' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_gather_table_n_partition_stats(), input parameter passed for in_table_nm cannot have null or empty value. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50007' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_gather_table_n_partition_stats(), input parameter in_partition_nm and in_partitition_val is NULL or empty ', CHR(10) , '. SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50008' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_gather_table_n_partition_stats(), input parameter passed for in_partition_nm or in_partition_val is empty string '''' it should be passed as NULL. ', CHR(10),  ' SQLERRM :- ' ,  SQLERRM),
				in_show_stack		=> TRUE 
			);
	WHEN SQLSTATE '50009' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in procedure :- p_util_gather_table_n_partition_stats(), input parameter passed for in_gather_stale_only_flg cannot have null. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE 
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in Procedure :- p_util_gather_table_n_partition_stats(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;