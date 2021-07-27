CREATE OR REPLACE PROCEDURE pub_admin.p_ctrl_log_error(
	in_taskexctn_id numeric,
	in_table_name character varying,
	in_task_id numeric,
	in_user_id character varying,
	in_system_err_cd character varying DEFAULT NULL::character varying,
	in_system_err_msg text DEFAULT NULL::text,
	in_stack_trace_details text DEFAULT NULL::text,
	in_user_err_msg character varying DEFAULT NULL::character varying,
	in_os_prcs_id character varying DEFAULT NULL::character varying)
LANGUAGE 'plpgsql'
SECURITY DEFINER 
AS $BODY$
   /* ----------------------------------------------------------------------------------
      -- Author  : GF8561
      -- Created : 01/03/2021
      -- Purpose : This is the Wrapper Procedure for Log_error.
      -----------------------------------------------------------------------------------
      -- Modification History
      -----------------------------------------------------------------------------------
      -- Ref#   Version#  Name                JIRA#    Date           Change Description
      -----------------------------------------------------------------------------------
      --  1.              Kalyan Kumar                01/03/2021      Initial Version
    ----------------------------------------------------------------------------------- */
DECLARE
	l_dblink_connection integer := 1;
    l_db_link_name      VARCHAR;
    l_log_mode          VARCHAR(30);
	v_return            varchar;
	co_log_mode CONSTANT varchar(10) DEFAULT 'LOG_MODE';
	co_log_mode_on  CONSTANT CHARACTER VARYING(10) DEFAULT 'ON';
BEGIN
    SELECT fnc_get_db_link_name() INTO STRICT l_db_link_name;
    BEGIN
		SELECT fnc_get_parameter(co_log_mode) INTO l_log_mode;
    EXCEPTION
        WHEN OTHERS THEN
            /*SBS_ERROR.ERROR_HANDLER(IN_ERROR_NO   => SQLCODE,
		    		    IN_ERROR_TXT  => pbl_constants_pkg.co_get_parameter_msg ||' .SQL Error Message - '||SQLERRM,
				    IN_SHOW_STACK => TRUE);*/
			RAISE EXCEPTION 'Error while calling the function fnc_get_parameter. SQL Error Message - %',SQLERRM;
    END;
	
    IF l_log_mode = co_log_mode_on THEN

		IF in_taskexctn_id IS NULL or in_taskexctn_id = 0 THEN
			RAISE EXCEPTION 'Task Execution ID cannot be NULL or 0.';
		END IF;

		IF in_task_id IS NULL or in_task_id = 0 THEN
			RAISE EXCEPTION 'Task ID cannot be NULL or 0.';
		END IF;

		IF in_user_id IS NULL THEN
			RAISE EXCEPTION 'User ID cannot be NULL.';
		END IF;

	    SELECT fnc_connect_to_db() into l_dblink_connection;

        IF l_dblink_connection = 0  THEN

			BEGIN
				--l_db_link_name := pbl_dbutil_pkg.l_db_link_name;  -- Ref#4
				select DBLINK_exec(l_db_link_name,'call prc_LOG_ERROR
				(in_taskexctn_id :=' || quote_nullable(in_taskexctn_id) || ',
				 In_table_name := ' || quote_nullable(in_table_name) || ',
				 in_task_id := ' || quote_nullable(in_task_id) || ',
				 in_system_err_cd := ' || quote_nullable(in_system_err_cd) || ',
				 in_system_err_msg := ' || quote_nullable(in_system_err_msg) || ',
				 in_stack_trace_details := ' || quote_nullable(in_stack_trace_details) || ',
				 in_user_err_msg := ' || quote_nullable(in_user_err_msg) || ' ,
				 in_os_prcs_id := ' || quote_nullable(in_os_prcs_id) || ',
				 in_user_id  := ' || quote_nullable(in_user_id) || ');') 
				 into v_return;
	    	EXCEPTION
				WHEN OTHERS THEN
				/*SBS_ERROR.ERROR_HANDLER(IN_ERROR_NO   => SQLCODE,
								IN_ERROR_TXT  => chr(10)||pbl_constants_pkg.co_dblink_execute_query_error_msg || 'in LOG_ERROR. SQL Error Message - '||SQLERRM,
							IN_SHOW_STACK => TRUE);*/
					RAISE EXCEPTION 'Error while executing query through DBLINK. SQL Error Message - %',SQLERRM;
	    	END;
        ELSE
	    	RAISE EXCEPTION 'Connection not estabilished';
        END IF;
	ELSE
		RAISE INFO 'Value of Parameter LOG_MODE in PBL_PARAM_T table is set to %. To Enter execution detail in Pub Admin Task Execution Table, It should be Set to ON.', l_log_mode;
	END IF;
EXCEPTION
    WHEN OTHERS THEN
	/*SBS_ERROR.ERROR_HANDLER(IN_ERROR_NO   => SQLCODE,
				IN_ERROR_TXT  => chr(10)||'Error While Processing : Task Execution ID - '||in_taskexctn_id||', Table Name - '||in_table_name||', Task ID - '||in_task_id ||', OS Prcs ID - '||in_os_prcs_id ||'. SQL Error Message - '||SQLERRM,
				IN_SHOW_STACK => TRUE);*/
		RAISE EXCEPTION 'Error Name:- Error in Procedure - P_CTRL_LOG_ERROR, While Processing : Task Execution ID - %, Table Name - %, Task ID - %, User Id - %, OS Prcs ID - %, SQL Error Message - %',in_taskexctn_id, in_table_name, in_task_id, in_user_id, in_os_prcs_id, SQLERRM;
        RAISE EXCEPTION 'Error State:%', SQLSTATE;
END;
$BODY$;
