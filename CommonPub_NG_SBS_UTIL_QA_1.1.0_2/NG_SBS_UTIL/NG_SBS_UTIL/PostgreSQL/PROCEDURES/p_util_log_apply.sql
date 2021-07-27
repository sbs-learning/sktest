CREATE OR REPLACE PROCEDURE sbs_util.P_UTIL_LOG_APPLY(
	IN_APPLY_METHOD CHARACTER VARYING,
	IN_VERSION CHARACTER VARYING,
	IN_DESCRIPTION CHARACTER VARYING,
	IN_COMMENTS CHARACTER VARYING,
	IN_VERSION_TABLE CHARACTER VARYING,
	IN_VERSION_SEQ CHARACTER VARYING DEFAULT NULL)
	
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE

   -------------------------------------------------------------------------------------------------------
   -- Name : P_UTIL_LOG_APPLY
   -------------------------------------------------------------------------------------------------------
   -- Description : Logs the validation (pre-check) for a version in sbs_util
   --               by calling LOG_APPLY
   --
   -- Logic : Verify the version is in a suitable state, and either insert or update
   --         into version table
   --
   --Input Parameter details
   --IN_APPLY_METHOD   - It can be INITIAL or UPGRADE
   --IN_VERSION        - Version to which the tool will be upgraded
   --IN_DESCRIPTION    - Description
   --IN_COMMENTS       - Comment about the release
   --IN_VERSION_TABLE  - Version table of the tool
   --IN_VERSION_SEQ    - Sequence on Version table of the tool
   ----------------------------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ----------------------------------------------------------------
   -- 	1	Akshay	     CSPUBCC-4301	04/08/2021	P_UTIL_LOG_APPLY() initial draft
   -- 	2	Akshay	     CSPUBCC-4383	04/15/2021	Added variable l_apply_method, l_version, l_description,
   --                                               l_comments, l_version_table, l_version_seq and upper/lower function in query.
   --   3   Akshay       CSPUBCC-4533   06/03/2021  Handle null and empty string for input parameters.
   ----------------------------------------------------------------------------------------------------
   
   l_apply_method       VARCHAR(10);
   l_version            VARCHAR(50);
   l_description        VARCHAR(100);
   l_comments           VARCHAR(500);
   l_version_table      VARCHAR(100);
   l_version_seq        VARCHAR(100);
   l_format_call_stack  TEXT;
   
 BEGIN

      l_apply_method  := UPPER(TRIM(IN_APPLY_METHOD));
      l_version       := TRIM(IN_VERSION);
      l_description   := TRIM(IN_DESCRIPTION);
      l_comments	  := TRIM(IN_COMMENTS);
	  l_version_table := LOWER(TRIM(IN_VERSION_TABLE));
	  l_version_seq   := LOWER(TRIM(IN_VERSION_SEQ));
	  
	  IF (l_apply_method IS NULL OR l_apply_method = '') THEN
	  	RAISE EXCEPTION null_value_not_allowed;
	  ELSIF (l_version IS NULL OR l_version = '') THEN
	  	RAISE EXCEPTION null_value_not_allowed;
	  ELSIF (l_description IS NULL OR l_description = '') THEN
	  	RAISE EXCEPTION null_value_not_allowed;
	  ELSIF (l_comments IS NULL OR l_comments = '') THEN
	  	RAISE EXCEPTION null_value_not_allowed;
	  ELSIF (l_version_table IS NULL OR l_version_table = '') THEN
	  	RAISE EXCEPTION null_value_not_allowed;
	  ELSIF (l_version_seq = '') THEN
	  	RAISE EXCEPTION null_value_not_allowed;
	  END IF;
	  
	  IF(l_apply_method NOT IN ('INITIAL','UPGRADE')) THEN
		RAISE EXCEPTION USING errcode = 50001;
	  end if;
		
      CALL sbs_util.LOG_APPLY (IN_APPLY_METHOD    => l_apply_method,
                      IN_VERSION         => l_version,
                      IN_DESCRIPTION     => l_description,
                      IN_COMMENTS        => l_comments,
                      IN_VERSION_TABLE   => l_version_table,
                      IN_VERSION_SEQ     => l_version_seq);
 EXCEPTION
   	WHEN null_value_not_allowed THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- p_util_log_apply(), Input in_apply_method / in_version / in_description / in_comments / in_version_table / in_version_seq parameter is NULL or Empty ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			); 
    WHEN SQLSTATE '50001' THEN
      GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;	
		CALL sbs_util.error_handler(
		in_error_stack => l_format_call_stack,
		in_error_code => SQLSTATE, 
		in_error_message => concat('Error in procedure :- p_util_log_apply(), Invalid value passed in parameter IN_APPLY_METHOD is: ',IN_APPLY_METHOD, '. This parameter should either have input value as INITIAL or UPGRADE. ', chr(10), 'SQLERRM:- ', SQLERRM),
		in_show_stack => TRUE);
		
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
			CALL sbs_util.error_handler(
					                    in_error_stack => l_format_call_stack, 
					                    in_error_code => SQLSTATE, 
					                    in_error_message => ' Error in procedure :- p_util_log_apply(), ' || chr(10) || 'SQLERRM:- ' || SQLERRM, 
					                    in_show_stack => TRUE
				                       );
END;
$BODY$;