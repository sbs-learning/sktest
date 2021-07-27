CREATE OR REPLACE PROCEDURE prc_log_error(
	in_taskexctn_id numeric,
	in_table_name character varying,
	in_task_id numeric,
	in_user_id character varying,
	in_system_err_msg text,
	in_stack_trace_details text,
	in_system_err_cd character varying DEFAULT NULL::character varying,
	in_user_err_msg character varying DEFAULT NULL::character varying,
	in_os_prcs_id character varying DEFAULT NULL::character varying)
LANGUAGE 'plpgsql'
AS $BODY$
   /* ----------------------------------------------------------------------------------
      -- Author  : GF8561
      -- Created : 01/03/2021
      -- Purpose : This is the Internal Procedure for Log_error. This will insert record into PBL_ERROR_T table.
      -----------------------------------------------------------------------------------
      -- Modification History
      -----------------------------------------------------------------------------------
      -- Ref#   Version#  Name                JIRA#    Date           Change Description
      -----------------------------------------------------------------------------------
      --  1.              Kalyan Kumar                01/03/2021      Initial Version
    ----------------------------------------------------------------------------------- */
DECLARE
	co_tsk_warning CONSTANT character varying(256) DEFAULT 'WARNING';
	co_error_task_logging_msg CONSTANT character varying(256) DEFAULT 'LOGGED BY ERROR_TASK API';
	co_error_task_warning_msg CONSTANT character varying(256) DEFAULT 'WARNING LOGGED BY ERROR API';
BEGIN
	INSERT INTO pbl_error_t
	(taskexctn_id,
	 task_id,
	 table_name,
	 err_ts,	
	 system_err_cd,
	 system_err_msg,
	 stack_trace_details,
	 user_err_msg,
	 os_prcs_id,
	 rcrd_create_ts,
	 rcrd_create_user_id)
	VALUES
	(in_taskexctn_id,in_task_id,in_table_name,CURRENT_TIMESTAMP,in_system_err_cd,in_system_err_msg,in_stack_trace_details,in_user_err_msg,in_os_prcs_id,CURRENT_TIMESTAMP,in_user_id);

      call prc_log_event(in_event_name            => co_tsk_warning,
                    in_taskexctn_id          => in_taskexctn_id,
                    in_table_name            => in_table_name,
                    in_task_id               => in_task_id,
                    in_user_id               => in_user_id,
                    in_event_src_cd_location => co_error_task_logging_msg,
                    in_event_dtl             => co_error_task_warning_msg,
                    in_event_statement       => NULL,
                    mtrc_cnt                 => NULL,
                    in_os_prcs_id            => in_os_prcs_id);
  
EXCEPTION
    WHEN OTHERS THEN
	/*SBS_ERROR.ERROR_HANDLER(IN_ERROR_NO   => SQLCODE,
				IN_ERROR_TXT  => 'Error While Processing : Task Execution ID - '||in_taskexctn_id||', Table Name - '||in_table_name||', Task ID - '||in_task_id ||', OS Prcs ID - '||in_os_prcs_id ||'. SQL Error Message - '||SQLERRM,
				IN_SHOW_STACK => TRUE);*/
		RAISE EXCEPTION 'Error Name:- Error in Procedure - PRC_LOG_ERROR, While Processing : Task Execution ID - %, Table Name - %, Task ID - %, User Id - %, OS Prcs ID - %, SQL Error Message - %',in_taskexctn_id, in_table_name, in_task_id, in_user_id, in_os_prcs_id, SQLERRM;
        RAISE EXCEPTION 'Error State:%', SQLSTATE;

END;
$BODY$;