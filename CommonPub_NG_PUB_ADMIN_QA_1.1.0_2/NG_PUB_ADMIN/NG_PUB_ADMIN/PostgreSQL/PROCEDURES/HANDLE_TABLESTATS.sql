CREATE OR REPLACE PROCEDURE pub_admin.handle_tablestats
(
	in_schema_nm				CHARACTER VARYING,
	in_table_nm					CHARACTER VARYING,
	in_user_id					CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING,
	in_gather_stale_only_flg	BOOLEAN DEFAULT FALSE
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
/* --------------------------------------------------------------------------
	-- Purpose : Purpose of this Procedure is to Analyze Table in a particular Schema.
	--in_schema_nm
	--in_table_nm
	--in_user_id  varying DEFAULT NULL
	--in_gather_stale_only_flg DEFAULT false
      -----------------------------------------------------------------------------------------------------------------------------*/
	l_table_exists          	VARCHAR(1);
	l_gathered_stats_flg		VARCHAR(1);
	l_format_call_stack			TEXT;
	co_analyze_table 			CONSTANT CHARACTER VARYING(256) DEFAULT 'ANALYZE TABLE';
	co_starting_analyze_table 	CONSTANT CHARACTER VARYING(256) DEFAULT 'STARTING ANALYZE TABLE';
	co_finish_analyze_table 	CONSTANT CHARACTER VARYING(256) DEFAULT 'FINISH ANALYZE TABLE';
	
	l_schema_nm 				VARCHAR(200);
	l_table_nm 					VARCHAR(200);
	l_gather_stale_only_flg 	BOOLEAN;
BEGIN

	l_schema_nm					:= LOWER(TRIM(in_schema_nm));
	l_table_nm					:= LOWER(TRIM(in_table_nm));
	l_gather_stale_only_flg		:= in_gather_stale_only_flg;

	CALL pub_admin.p_ctrl_log_event
		(
			in_event_constant           => 'CO_ANALYZE_TABLE',
			in_event_dtl             	=> co_starting_analyze_table,
			in_table_name            	=> l_table_nm,
			in_user_id               	=> in_user_id,
			in_event_src_cd_location 	=> 'handle_tablestats',
			in_event_statement       	=> 'ANALYZE'
		);
	CALL sbs_util.gather_table_stats
		(
			in_schema_nm       			=> l_schema_nm,
            in_table_nm            		=> l_table_nm,
            in_gather_stale_only_flg 	=> l_gather_stale_only_flg,
            out_gathered_stats_flg  	=> l_gathered_stats_flg
		);
	
	IF(l_gathered_stats_flg = 'Y') THEN
		RAISE INFO 'Analyzed Table';
	ELSE
		RAISE INFO 'Not Analyzed Table';
	END IF;
	
	CALL pub_admin.p_ctrl_log_event
		(
			in_event_constant           => 'CO_ANALYZE_TABLE',
			in_event_dtl             	=> co_finish_analyze_table,
			in_table_name            	=> l_table_nm,
			in_user_id               	=> in_user_id,
			in_event_src_cd_location 	=> 'handle_tablestats',
			in_event_statement       	=> 'ANALYZE'
		);
EXCEPTION
    WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in Procedure :- handle_tablestats(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
