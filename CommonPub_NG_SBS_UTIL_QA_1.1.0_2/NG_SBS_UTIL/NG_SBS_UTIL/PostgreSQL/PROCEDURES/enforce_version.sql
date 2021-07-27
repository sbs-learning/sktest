CREATE OR REPLACE PROCEDURE sbs_util.enforce_version(
	in_from_version character varying,
	in_to_version character varying,
	in_version_table character varying)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
      -------------------------------------------------------------------------------------
      -- Name : enforce_version
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
      -- 	1	Akshay	     CSPUBCC-4301	04/08/2021	enforce_version() initial draft
      -- 	2	Akshay	     CSPUBCC-4383	04/15/2021	Added variable l_from_version, l_to_version, l_version_table
      --                                               and lower function in query
      -------------------------------------------------------------------------------------
      l_actual_version            VARCHAR;
      L_VERIFICATION_DATE         timestamptz;
      L_APPLIED_DATE              timestamptz;
      l_sql_txt                   VARCHAR(32767);
	  co_single_quote             VARCHAR(1) DEFAULT CHR(39);
	  verification_hour_limit     timestamptz;
	  l_from_version              VARCHAR(100);
	  l_to_version                VARCHAR(100);
	  l_version_table             VARCHAR(100);
	  l_format_call_stack         Text;
	  
 BEGIN
      l_from_version  := TRIM(IN_FROM_VERSION);
	  l_to_version    := TRIM(IN_TO_VERSION);
	  l_version_table := LOWER(TRIM(IN_VERSION_TABLE));
   
       select current_date - interval '3 hour' into verification_hour_limit;
      
      l_actual_version := SBS_UTIL.last_APPLIED_VERSION_FNC (l_version_table);

    IF l_actual_version IS NULL THEN
		RAISE EXCEPTION USING errcode = 50001;
    ELSE
        IF l_actual_version <> l_from_version THEN
		  RAISE EXCEPTION USING errcode = 50002;
        END IF;
    END IF;

    IF l_to_version IS NOT NULL THEN

        L_SQL_TXT:='SELECT MAX (VERIFICATION_DATE), MAX (APPLIED_DATE)
           FROM '||l_version_table||
          ' WHERE version ='||co_single_quote||l_to_version||co_single_quote;
		  
		  RAISE INFO 'Executing: %',L_SQL_TXT;
		  
        EXECUTE L_SQL_TXT INTO L_VERIFICATION_DATE,L_APPLIED_DATE;
       
        IF l_applied_date IS NOT NULL THEN
		  RAISE EXCEPTION USING errcode = 50003;
        ELSE
            IF l_VERIFICATION_DATE IS NULL
			OR l_VERIFICATION_DATE < verification_hour_limit THEN
			  RAISE EXCEPTION USING errcode = 50004;
            END IF;
        END IF;
    END IF;
EXCEPTION

    WHEN SQLSTATE '50001' THEN
	  GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler(
		in_error_stack => l_format_call_stack,
		in_error_code => SQLSTATE, 
		in_error_message => concat('Error in procedure :- enforce_version(),parameter :- in_from_version, No current version found' , in_from_version ,  chr(10) , ' SQLERRM:- ' , SQLERRM),
		in_show_stack => TRUE);
		
	WHEN SQLSTATE '50002' THEN
	  GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler(
		in_error_stack => l_format_call_stack,
		in_error_code => SQLSTATE, 
		in_error_message => concat('Error in procedure :- enforce_version(),parameter in_from_version', in_from_version, 'is not equal to l_actual_version' , l_actual_version ,  chr(10) , ' SQLERRM:- ' , SQLERRM),
		in_show_stack => TRUE);
		
	WHEN SQLSTATE '50003' THEN
      GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;	
		CALL sbs_util.error_handler(
		in_error_stack => l_format_call_stack,
		in_error_code => SQLSTATE, 
		in_error_message => 'Error in procedure :- enforce_version(),parameter l_applied_date:- Version requested has already been applied. ' || chr(10) || 'SQLERRM:- ' || SQLERRM,
		in_show_stack => TRUE);
		
	WHEN SQLSTATE '50004' THEN
      GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;		
		CALL sbs_util.error_handler(
        in_error_stack => l_format_call_stack,		
		in_error_code => SQLSTATE, 
		in_error_message => 'Error in procedure :- enforce_version(),parameter l_verification_date:- Version requested has not been verified within the past 3 hours. ' || chr(10) || 'SQLERRM:- ' || SQLERRM,
		in_show_stack => TRUE);

    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
			CALL sbs_util.error_handler(
					                    in_error_stack => l_format_call_stack, 
					                    in_error_code => SQLSTATE, 
					                    in_error_message => ' Error in procedure :- enforce_version(), ' || chr(10) || 'SQLERRM:- ' || SQLERRM, 
					                    in_show_stack => TRUE
				                       );
END;
$BODY$;
