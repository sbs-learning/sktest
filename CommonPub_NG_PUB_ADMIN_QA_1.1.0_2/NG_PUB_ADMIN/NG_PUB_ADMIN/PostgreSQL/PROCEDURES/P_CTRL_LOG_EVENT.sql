CREATE OR REPLACE PROCEDURE pub_admin.p_ctrl_log_event
(
	in_event_constant 			CHARACTER VARYING,
	in_table_name 				CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING,
	in_event_src_cd_location 	CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING,
	in_event_statement 			TEXT DEFAULT NULL::TEXT,
	in_event_dtl 				TEXT DEFAULT NULL::CHARACTER VARYING,
	in_user_id 					CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING
)
LANGUAGE 'plpgsql'
SECURITY DEFINER 
AS $BODY$
/* ----------------------------------------------------------------------------------
      -- Author  : GF8561
      -- Created : 01/03/2021
      -- Purpose : This is the Wrapper Procedure for log_event.
      -----------------------------------------------------------------------------------
      -- Modification History
      -----------------------------------------------------------------------------------
      -- Ref#   Version#  Name                JIRA#    Date           Change Description
      -----------------------------------------------------------------------------------
      --  1.              Kalyan Kumar                01/03/2021      Initial Version
	  --  2.              Kalyan Kumar                06/07/2021      Removed usage of DB_link in Event logging. It is planned to use pg_fdw for Autonomous transaction. 
	  --                                                              It will be available in future release
	  --															  After this change event will not be logged.
	  --                                                                1. If Log Mode parameter is set to OFF in PBA_PARAM_T table.
	  --                                                                2. If the transaction fails.
    ----------------------------------------------------------------------------------- */
DECLARE
	
    l_log_mode           	VARCHAR(30);
	co_log_mode    CONSTANT VARCHAR(10) DEFAULT 'LOG_MODE';
	co_log_mode_on CONSTANT VARCHAR(10) DEFAULT 'ON';
	l_event_constant    	VARCHAR(200);
	l_event_name			VARCHAR(200);
	l_table_name        	VARCHAR(200);
	l_event_src_cd_location VARCHAR(200);
	l_user_id 				VARCHAR(100);
	l_event_statement 		TEXT;
	l_event_dtl 			TEXT;
	l_format_call_stack 	TEXT;
BEGIN

	l_event_constant 		:= TRIM(UPPER(in_event_constant));
	l_table_name 			:= TRIM(in_table_name);
	l_event_src_cd_location := TRIM(in_event_src_cd_location);
	l_event_statement 		:= TRIM(in_event_statement);
	l_event_dtl 			:= TRIM(in_event_dtl);
	l_user_id 				:= TRIM(in_user_id);

	BEGIN
		SELECT pub_admin.get_parameter(co_log_mode) INTO STRICT l_log_mode;
    EXCEPTION
        WHEN OTHERS THEN
			RAISE EXCEPTION USING ERRCODE = 50001;
    END;
	
    IF l_log_mode = co_log_mode_on THEN
		
        IF l_event_constant IS NULL THEN
			RAISE EXCEPTION USING ERRCODE = 50002;
		END IF;

		BEGIN
			SELECT event_name
			  INTO STRICT l_event_name
			  FROM pub_admin.pba_event_name_t 
			 WHERE TRIM(UPPER(event_constant)) = l_event_constant;
		EXCEPTION
			WHEN NO_DATA_FOUND THEN
				RAISE EXCEPTION USING errcode = 50006;
		END;

		CALL pub_admin.prc_log_event
			(
				in_event_name				=> l_event_name,
				in_table_name				=> l_table_name,
				in_event_src_cd_location		=> l_event_src_cd_location,
				in_event_statement			=> l_event_statement,
				in_event_dtl				=> l_event_dtl,
				in_user_id				=> l_user_id
			);
	ELSE
		RAISE INFO 'Event Logging is disabled as Log Mode is set to OFF in PBA_PARAM_T table.';
	END IF;
EXCEPTION
    WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- p_ctrl_log_event(), Error while calling the function get_parameter. ',CHR(10),' SQLERRM :- ',SQLERRM),
				in_show_stack		=> TRUE
			);
    WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- p_ctrl_log_event(), Parameter in_event_constant cannot have Null or empty value. ',CHR(10),' SQLERRM :- ',SQLERRM),
				in_show_stack		=> TRUE
			);
    WHEN SQLSTATE '50006' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- p_ctrl_log_event(), No Event Name found for this constant - ',in_event_constant, CHR(10),' SQLERRM :- ',SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_ctrl_log_event() ' ,CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
