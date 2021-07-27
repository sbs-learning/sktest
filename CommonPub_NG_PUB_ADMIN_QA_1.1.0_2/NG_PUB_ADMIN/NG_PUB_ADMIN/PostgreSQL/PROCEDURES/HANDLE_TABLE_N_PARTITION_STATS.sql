CREATE OR REPLACE PROCEDURE pub_admin.handle_table_n_partition_stats
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
AS $BODY$
DECLARE
	l_table_exists          	VARCHAR(1);
	l_gathered_stats_flg		VARCHAR(1);
	l_partition_ind 			VARCHAR(1);
	l_get_partition_nm			VARCHAR(100);
	l_format_call_stack			TEXT;
	co_analyze_table 			CONSTANT CHARACTER VARYING(256) DEFAULT 'ANALYZE TABLE';
	co_starting_analyze_table 	CONSTANT CHARACTER VARYING(256) DEFAULT 'STARTING ANALYZE TABLE';
	co_finish_analyze_table 	CONSTANT CHARACTER VARYING(256) DEFAULT 'FINISH ANALYZE TABLE';
	co_analyze_table_partition 	CONSTANT CHARACTER VARYING(256) DEFAULT 'ANALYZE TABLE PARTITION';
	co_starting_analyze_table_partition CONSTANT CHARACTER VARYING(256) DEFAULT 'STARTING ANALYZE TABLE PARTITION';
	co_finish_analyze_table_partition CONSTANT CHARACTER VARYING(256) DEFAULT 'FINISH ANALYZE TABLE PARTITION';

	l_schema_nm 				VARCHAR(250);
	l_table_nm 					VARCHAR(250);
	l_partition_nm 				VARCHAR(250);
	l_partition_val 			VARCHAR(250);
BEGIN

	l_schema_nm 		:= LOWER(TRIM(in_schema_nm));
	l_table_nm 			:= LOWER(TRIM(in_table_nm));
	l_partition_nm 		:= LOWER(TRIM(in_partition_nm));
	l_partition_val 	:= LOWER(TRIM(in_partition_val));

	IF (l_partition_nm IS NOT NULL AND l_partition_val IS NOT NULL)
	THEN
		RAISE EXCEPTION USING ERRCODE = 50004;
    END IF;

	IF (l_partition_nm IS NULL AND l_partition_val IS NULL)
	THEN
		CALL pub_admin.p_ctrl_log_event
			(
				in_event_constant   		=> 'CO_ANALYZE_TABLE_N_PARTITION',
				in_event_dtl             	=> co_starting_analyze_table,
				in_table_name            	=> l_table_nm,
				in_user_id               	=> in_user_id,
				in_event_src_cd_location 	=> 'handle_table_n_partition_stats',
				in_event_statement       	=> 'ANALYZE'
			);

		CALL pub_admin.handle_tablestats
			(
				in_schema_nm    			=> l_schema_nm,
				in_table_nm     			=> l_table_nm,
				in_user_id      			=> in_user_id,
				in_gather_stale_only_flg 	=> in_gather_stale_only_flg
			);

		CALL pub_admin.p_ctrl_log_event
			(
				in_event_constant   		=> 'CO_ANALYZE_TABLE_N_PARTITION',
				in_event_dtl             	=> co_finish_analyze_table,
				in_table_name            	=> l_table_nm,
				in_user_id               	=> in_user_id,
				in_event_src_cd_location 	=> 'handle_table_n_partition_stats',
				in_event_statement       	=> 'ANALYZE'
			);
		
	ELSIF (l_partition_nm IS NOT NULL) THEN
		CALL pub_admin.p_ctrl_log_event
			(
				in_event_constant   		=> 'CO_ANALYZE_TABLE_N_PARTITION',
				in_event_dtl             	=> co_starting_analyze_table_partition,
				in_table_name            	=> l_table_nm,
				in_user_id               	=> in_user_id,
				in_event_src_cd_location 	=> 'handle_table_n_partition_stats',
				in_event_statement       	=> 'ANALYZE'
			);
		
        CALL sbs_util.gather_table_partition_stats
			(
				in_schema_nm 				=> l_schema_nm,
				in_table_nm 				=> l_table_nm,
				in_partition_nm 			=> l_partition_nm,
				in_gather_stale_only_flg 	=> in_gather_stale_only_flg,
				out_gathered_stats_flg 		=> l_gathered_stats_flg
			);
		
		IF (l_gathered_stats_flg = 'Y') THEN
			RAISE INFO 'Analyzed Table Partition';
		ELSE
			RAISE INFO 'Not Analyzed Table Partition';
		END IF;
						
		CALL pub_admin.p_ctrl_log_event
			(
				in_event_constant   		=> 'CO_ANALYZE_TABLE_N_PARTITION',
				in_event_dtl             	=> co_finish_analyze_table_partition,
				in_table_name            	=> l_table_nm,
				in_user_id               	=> in_user_id,
				in_event_src_cd_location 	=> 'handle_table_n_partition_stats',
				in_event_statement       	=> 'ANALYZE'
			);
	
	ELSIF (l_partition_val IS NOT NULL)
	THEN
		l_get_partition_nm := pub_admin.get_partition_name(l_schema_nm, l_table_nm, l_partition_val, in_subpartition_val);
		RAISE INFO 'l_get_partition_nm - %',l_get_partition_nm;
		CALL pub_admin.p_ctrl_log_event
			(
				in_event_constant  			=> 'CO_ANALYZE_TABLE_N_PARTITION',
				in_event_dtl             	=> co_starting_analyze_table_partition,
				in_table_name            	=> l_table_nm,
				in_user_id               	=> in_user_id,
				in_event_src_cd_location 	=> 'handle_table_n_partition_stats',
				in_event_statement       	=> 'ANALYZE'
			);
		
		CALL sbs_util.gather_table_partition_stats
			(
				in_schema_nm 				=> l_schema_nm,
				in_table_nm 				=> l_table_nm,
				in_partition_nm 			=> l_get_partition_nm,
				in_gather_stale_only_flg 	=> in_gather_stale_only_flg,
				out_gathered_stats_flg 		=> l_gathered_stats_flg
			);
		
		IF (l_gathered_stats_flg = 'Y') THEN
			RAISE INFO 'Analyzed Table Partition';
		ELSE
			RAISE INFO 'Not Analyzed Table Partition';
		END IF;

		CALL pub_admin.p_ctrl_log_event
			(
				in_event_constant   		=> 'CO_ANALYZE_TABLE_N_PARTITION',
				in_event_dtl             	=> co_finish_analyze_table_partition,
				in_table_name            	=> l_table_nm,
				in_user_id               	=> in_user_id,
				in_event_src_cd_location 	=> 'handle_table_n_partition_stats',
				in_event_statement       	=> 'ANALYZE'
			);
    END IF;
EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- handle_table_n_partition_stats(), Table does not exists. ', CHR(10), ' SQLERRM :-  ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- handle_table_n_partition_stats(), input parameter in_schema_nm cannot have null or empty value. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- handle_table_n_partition_stats(), input parameter in_table_nm cannot have null or empty value. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in function :- handle_table_n_partition_stats(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;