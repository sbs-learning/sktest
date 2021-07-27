CREATE OR REPLACE PROCEDURE sbs_util.P_UTIL_ENFORCE_VERSION
(
	IN_FROM_VERSION		CHARACTER VARYING,
	IN_TO_VERSION		CHARACTER VARYING,
	IN_VERSION_TABLE	CHARACTER VARYING
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
      -------------------------------------------------------------------------------------
      -- Name : P_UTIL_ENFORCE_VERSION
      -------------------------------------------------------------------------------------
      -- Description : Checks current version and raises appropriate exception if
      --               it does not match.
      --
      -- Logic : Verify the version is in a suitable state, and either insert or update
      --         into Version table
      --
      --Input Parameter details
      --IN_FROM_VERSION   - Current version of the tool
      --IN_TO_VERSION     - Version to which the tool will be upgraded
      --IN_VERSION_TABLE  - Version table of the tool
	  --------------------------------------------------------------------------------
      -- RefNo Name            JIRA NO 		Date     Description of change
      -- ----- ---------------- -------- ---------------------------------------------
      -- 	1	Akshay	     CSPUBCC-4301	04/08/2021	P_UTIL_ENFORCE_VERSION() initial draft
      -- 	2	Akshay	     CSPUBCC-4383	04/15/2021	Added variable l_from_version, l_to_version, l_version_table
      --                                                and lower function in query.
	  --    3   Akshay       CSPUBCC-4533   06/03/2021  Handle null and empty string for input parameters.
      -------------------------------------------------------------------------------------
	  
	  l_from_version        VARCHAR(50);
	  l_to_version          VARCHAR(50);
	  l_version_table       VARCHAR(100);
	  l_format_call_stack   Text;
	  
BEGIN
	l_from_version  := TRIM(IN_FROM_VERSION);
	l_to_version    := TRIM(IN_TO_VERSION);
	l_version_table := LOWER(TRIM(IN_VERSION_TABLE));
	
	IF (l_from_version IS NULL OR l_from_version = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	ELSIF (l_to_version IS NULL OR l_to_version = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	ELSIF (l_version_table IS NULL OR l_version_table = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	END IF;
	  
	CALL sbs_util.ENFORCE_VERSION
		(
			IN_FROM_VERSION    => l_from_version,
			IN_TO_VERSION      => l_to_version,
			IN_VERSION_TABLE   => l_version_table
		);

EXCEPTION
	WHEN null_value_not_allowed THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_enforce_version(), Input in_from_version / in_to_version / in_version_table parameter is NULL or Empty ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			); 
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in procedure :- p_util_enforce_version(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;