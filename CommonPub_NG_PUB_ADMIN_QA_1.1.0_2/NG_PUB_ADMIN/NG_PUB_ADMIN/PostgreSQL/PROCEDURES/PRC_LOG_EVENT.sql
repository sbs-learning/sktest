CREATE OR REPLACE PROCEDURE pub_admin.prc_log_event(
	in_event_name	 			CHARACTER VARYING,
	in_table_name 				CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING,
	in_event_src_cd_location 	CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING,
	in_event_statement TEXT 	DEFAULT NULL::TEXT,
	in_event_dtl TEXT 			DEFAULT NULL::CHARACTER VARYING,
	in_user_id 					CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING)
LANGUAGE 'plpgsql'
AS $BODY$
/* ----------------------------------------------------------------------------------
      -- Author  : GF8561
      -- Created : 01/03/2021
      -- Purpose : This is the Internal Procedure for Log_event. This will insert record into PBA_EVENT_T table.
      -----------------------------------------------------------------------------------
      -- Modification History
      -----------------------------------------------------------------------------------
      -- Ref#   Version#  Name                JIRA#    Date           Change Description
      -----------------------------------------------------------------------------------
      --  1.              Kalyan Kumar                01/03/2021      Initial Version
    ----------------------------------------------------------------------------------- */
DECLARE
    l_event_name        	VARCHAR(256);
	l_ip_address        	VARCHAR(30);
	l_user_id 				VARCHAR(100);
	l_table_name        	VARCHAR(200);
	l_event_src_cd_location VARCHAR(200);
	l_event_statement 		TEXT;
	l_event_dtl 			TEXT;
	l_format_call_stack 	TEXT;
	
BEGIN

	l_event_name 			:= TRIM(in_event_name);
	l_table_name 			:= TRIM(in_table_name);
	l_event_src_cd_location := TRIM(in_event_src_cd_location);
	l_event_statement 		:= TRIM(in_event_statement);
	l_event_dtl 			:= TRIM(in_event_dtl);
	l_user_id 				:= TRIM(in_user_id);
	
	-- Fetch IP add of the machine
	l_ip_address := inet_client_addr();

	INSERT INTO pub_admin.pba_event_t
	(
		event_name,
		table_name,
		event_src_cd_location,
		event_statement,
		event_dtl,
		rcrd_create_ts,
		rcrd_create_ip,
		rcrd_create_user_id
	)
	VALUES
	(
		l_event_name,
		l_table_name,
	 	l_event_src_cd_location,
		l_event_statement,
		l_event_dtl,
		CURRENT_TIMESTAMP,
		l_ip_address,
		COALESCE(l_user_id,sbs_util.get_audit_user())
    );

EXCEPTION
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in Procedure :- prc_log_event() ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
