CREATE OR REPLACE FUNCTION pub_admin.get_parameter
(
	in_param_nm CHARACTER VARYING
)
    RETURNS CHARACTER VARYING
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
/* 
   --------------------------------------------------------------------------------
   -- Name: get_parameter
   --------------------------------------------------------------------------------
   --
   -- Description: Purpose of this function is to fetch the param value text from PBA_PARAM_T table 
                   against the param Name that is provided in parameter value.
   --
   --------------------------------------------------------------------------------
   -- RefNo Name             JIRA NO 		 Date         Description of change
   -- ----- ---------------- -------- ------------------ ---------------------------
   -- 	1	Akshay	        CSPUBCC-4305	        	   get_parameter() initial draft
*/   

	l_param_nm           VARCHAR(500);
	l_txt                VARCHAR;
	l_format_call_stack  TEXT;
	l_param_exist        SMALLINT;
BEGIN

	l_param_nm := TRIM(in_param_nm);
	
    IF (l_param_nm IS NULL OR l_param_nm = '') THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
    END IF;
	
	SELECT COUNT(1) INTO l_param_exist FROM pub_admin.pba_param_t WHERE PARAM_NM = l_param_nm;
	
	IF l_param_exist = 0 AND UPPER(l_param_nm) LIKE 'AFHINT_%' THEN
		RETURN NULL;
	END IF;
	
	IF (l_param_exist = 1) THEN
		SELECT param_value_txt INTO l_txt FROM pub_admin.pba_param_t WHERE PARAM_NM = l_param_nm;
	ELSE
		RAISE EXCEPTION USING ERRCODE = 50002;
	END IF;
	
    RETURN L_TXT;

EXCEPTION
	WHEN SQLSTATE '50001' THEN  
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in function :- get_Parameter(), input parameter in_param_nm cannot be null or empty' , CHR(10), 'SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in function :- get_Parameter(),parameter ', in_param_nm, ' not found in table pba_param_t. ', CHR(10), ' SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in function :- get_Parameter()', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;