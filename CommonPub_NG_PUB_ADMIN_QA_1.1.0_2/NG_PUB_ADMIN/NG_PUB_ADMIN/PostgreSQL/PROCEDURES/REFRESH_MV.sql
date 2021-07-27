CREATE OR REPLACE PROCEDURE pub_admin.refresh_mv
(
	in_schema_nm		CHARACTER VARYING,
	in_mv_nm			CHARACTER VARYING,
	with_data_flg		CHARACTER VARYING DEFAULT 'Y'::CHARACTER VARYING
)
LANGUAGE 'plpgsql'
	-----------------------------------------------------------------------------
   -- Name :REFRESH_MV                                             
   -----------------------------------------------------------------------------
   -- Description : To refresh mv of a particular schema.
   -- Parameters: IN_SCHEMA_NM, IN_MVIEW_NM, WITH_DATA_FLG
   ------------------------------------------------------------------------------
   -- RefNo  Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- -------------------------------------------
   -- 1      Akshay        CSPUBCC-4383   4/15/2021  Added variable 
   --                                                removed upper function from query and added exception block.                                                                                                  
   ------------------------------------------------------------------------------
AS $BODY$
DECLARE
	l_format_call_stack		TEXT;
	co_refresh_mv			CONSTANT CHARACTER VARYING(256) DEFAULT 'REFRESH MV';
	co_starting_refresh_mv	CONSTANT CHARACTER VARYING(256) DEFAULT 'STARTING REFRESH MV';
	co_finish_refresh_mv	CONSTANT CHARACTER VARYING(256) DEFAULT 'FINISH REFRESH MV';
	l_table_exists			VARCHAR(1);
	
	l_schema_nm				CHARACTER VARYING(100);
	l_mv_nm					CHARACTER VARYING(100);
	l_with_data_flg			CHARACTER VARYING(1);
BEGIN
	l_schema_nm				:= LOWER(TRIM(in_schema_nm));
	l_mv_nm					:= LOWER(TRIM(in_mv_nm));
	l_with_data_flg			:= UPPER(TRIM(with_data_flg));

	IF l_schema_nm IS NOT NULL AND l_mv_nm IS NOT NULL and l_schema_nm != '' and l_mv_nm != '' THEN
		BEGIN
			SELECT 'Y'
              INTO STRICT l_table_exists 
			  FROM pg_matviews
			 WHERE schemaname = l_schema_nm
			   AND matviewname = l_mv_nm;
			RAISE INFO 'l_table_exists - %',l_table_exists;
		EXCEPTION
        	WHEN NO_DATA_FOUND THEN
				RAISE EXCEPTION USING ERRCODE = 50001;
		END;
	ELSIF l_schema_nm IS NULL or l_schema_nm = '' THEN
		 RAISE EXCEPTION USING ERRCODE = 50002;
	ELSIF l_mv_nm IS NULL or l_mv_nm = '' THEN
		RAISE EXCEPTION USING ERRCODE = 50003;
    END IF;

	CALL pub_admin.p_ctrl_log_event
		(
			in_event_constant			=> 'CO_REFRESH_MV',
			in_table_name				=> l_mv_nm,
			in_event_src_cd_location	=> 'refresh_mv',
			in_event_statement			=> 'REFRESH MV',
			in_event_dtl				=> co_starting_refresh_mv,
			in_user_id					=> sbs_util.get_audit_user()
		);

	CALL sbs_util.refresh_mv 
		(
			in_schema_nm				=> l_schema_nm,
			in_mview_name_lst			=> l_mv_nm,
			with_data_flg				=> l_with_data_flg);

    EXECUTE 'GRANT SELECT ON ' || l_schema_nm || '.' || l_mv_nm || ' TO PUBLIC';

	CALL pub_admin.p_ctrl_log_event 
		(
			in_event_constant			=> 'CO_REFRESH_MV',
			in_table_name				=> l_mv_nm,
			in_event_src_cd_location	=> 'refresh_mv',
			in_event_statement			=> 'REFRESH MV',
			in_event_dtl				=> co_finish_refresh_mv,
			in_user_id					=> sbs_util.get_audit_user()
		);
EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- refresh_mv(), MV ', in_mv_nm, ' does not exists in schema ', in_schema_nm, CHR(10), '. SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- refresh_mv(), input parameter in_schema_nm cannot have null or empty value. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- refresh_mv(), input parameter in_mv_nm cannot have null or empty value. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- refresh_mv(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
