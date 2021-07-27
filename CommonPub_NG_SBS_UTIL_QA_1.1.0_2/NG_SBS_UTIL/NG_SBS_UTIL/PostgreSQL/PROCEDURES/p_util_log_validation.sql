CREATE OR REPLACE PROCEDURE sbs_util.P_UTIL_LOG_VALIDATION
(
	IN_VERSION			CHARACTER VARYING,
	IN_DESCRIPTION		CHARACTER VARYING,
	IN_COMMENT			CHARACTER VARYING,
	IN_VERSION_TABLE	CHARACTER VARYING,
	IN_VERSION_SEQ		CHARACTER VARYING
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   -------------------------------------------------------------------------------------
   -- Name : P_UTIL_LOG_VALIDATION
   -------------------------------------------------------------------------------------
   -- Description : Logs the validation (pre-check) for a version in the specified version table
   --               by calling LOG_VALIDATION
   --
   -- Logic : Verify the version is in a suitable state, and either insert or update
   --         into Version table
   --
   --Input Parameter details
   --in_version        - Version to which the tool will be upgraded
   --IN_DESCRIPTION    - Description
   --IN_COMMENT        - Comment about the release
   --IN_VERSION_TABLE  - Version table of the tool
   --IN_VERSION_SEQ    - Sequence on Version table of the tool
   -----------------------------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ------------------------------------------------------------------
   -- 	1	Akshay	     CSPUBCC-4301	04/08/2021	log_validation() initial draft
   -- 	2	Akshay	     CSPUBCC-4383	04/15/2021	Added variable l_version, l_description, l_comment,
   --                                               l_version_table, l_version_seq and lower function in query.
   --   3   Akshay       CSPUBCC-4533   06/03/2021  Handle null and empty string for input parameters.
   -------------------------------------------------------------------------------------

   l_version             VARCHAR(50);
   l_description         VARCHAR(100);
   l_comment             VARCHAR(500);
   l_version_table       VARCHAR(100);
   l_version_seq         VARCHAR(100);
   l_format_call_stack   TEXT;

BEGIN
	l_version       := TRIM(IN_VERSION);
	l_description   := TRIM(IN_DESCRIPTION);
	l_comment       := TRIM(IN_COMMENT);
	l_version_table := LOWER(TRIM(IN_VERSION_TABLE));
	l_version_seq   := LOWER(TRIM(IN_VERSION_SEQ));
	
	IF (l_version IS NULL OR l_version = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	ELSIF (l_description IS NULL OR l_description = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	ELSIF (l_comment IS NULL OR l_comment = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	ELSIF (l_version_table IS NULL OR l_version_table = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	ELSIF (l_version_seq IS NULL OR l_version_seq = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	END IF;

	CALL sbs_util.LOG_VALIDATION
		(
			IN_VERSION         => l_version,
			IN_DESCRIPTION     => l_description,
			IN_COMMENT         => l_comment,
			IN_VERSION_TABLE   => l_version_table,
			IN_VERSION_SEQ     => l_version_seq
		);
EXCEPTION
	WHEN null_value_not_allowed THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_log_validation(), Input IN_VERSION / IN_DESCRIPTION / IN_COMMENT / IN_VERSION_TABLE / IN_VERSION_SEQ parameter is NULL or Empty ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			); 
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_log_validation(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;