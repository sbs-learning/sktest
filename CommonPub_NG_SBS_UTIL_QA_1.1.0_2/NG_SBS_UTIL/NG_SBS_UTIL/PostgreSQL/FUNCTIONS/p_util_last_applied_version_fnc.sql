CREATE OR REPLACE FUNCTION sbs_util.P_UTIL_LAST_APPLIED_VERSION_FNC
(
	IN_VERSION_TABLE CHARACTER VARYING
)
RETURNS VARCHAR
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
      -------------------------------------------------------------------------------------
      -- Name : P_UTIL_LAST_APPLIED_VERSION_FNC
      -------------------------------------------------------------------------------------
      -- Description : Returns the most-recently applied version by calling LAST_APPLIED_VERSION_FNC 
      --
      -- Logic : Query  Version Table and return version of last applied version
      --
      --Input Parameter details
      --IN_VERSION_TABLE  - Version table of the tool
	  --------------------------------------------------------------------------------
      -- RefNo Name            JIRA NO 		Date     Description of change
      -- ----- ---------------- -------- ---------------------------------------------
      -- 	1	Akshay	     CSPUBCC-4301	04/08/2021	P_UTIL_LAST_APPLIED_VERSION_FNC() initial draft
      -- 	2	Akshay	     CSPUBCC-4383	04/15/2021	Added variable l_version_table
      --                                                and lower function in query.
	  --    3   Akshay       CSPUBCC-4533   06/04/2021  Handle empty string for input parameters.
      -------------------------------------------------------------------------------------
      l_last_applied_version   VARCHAR;
	  l_version_table          VARCHAR(100);
	  l_format_call_stack      TEXT;
	  
   BEGIN
       l_version_table := LOWER(TRIM(IN_VERSION_TABLE));

      IF (l_version_table IS NULL OR l_version_table = '') THEN
		 RAISE EXCEPTION USING errcode = 50001;
      END IF;	   
   
      l_last_applied_version :=
         SBS_UTIL.LAST_APPLIED_VERSION_FNC (
            IN_VERSION_TABLE => l_version_table);
			
      RETURN l_last_applied_version;
   EXCEPTION
   	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;	
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,		
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in function :- p_util_last_applied_version_fnc(), input parameter in_version_table not accept null or empty value. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);  
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
			CALL sbs_util.error_handler
				(
					in_error_stack => l_format_call_stack, 
					in_error_code => SQLSTATE, 
					in_error_message => CONCAT(' Error in procedure :- p_util_last_applied_version_fnc(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
					in_show_stack => TRUE
				);
END;
$BODY$;