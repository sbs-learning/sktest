CREATE OR REPLACE FUNCTION sbs_util.p_util_get_numeric_version
(
	in_version 		CHARACTER VARYING
)
RETURNS numeric
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    -------------------------------------------------------------------------------------
    -- Name : p_util_get_numeric_version
    -------------------------------------------------------------------------------------
    -- Description : To get the numeric value of any version.
    -- Parameters : in_version, in_max_digs
	----------------------------------------------------------------------------------
    -- RefNo Name            JIRA NO 		   Date           Description of change
    -- ----- ---------------- -------- ---------------------------------------------
    -- 	1	Kalyan  	    CSPUBCC-4401	04/19/2021     	  Added exception block in the query.
	--  2   Akshay          CSPUBCC-4533    06/04/2021        Handle empty string for input parameters.
    -------------------------------------------------------------------------------------
    nversionval         	numeric;
	l_format_call_stack  	TEXT;
	
BEGIN
    IF (in_version IS NULL OR in_version = '') THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;

	nversionval := sbs_util.get_numeric_version 
				(
					in_version		=> in_version
				);
	
	RETURN nversionval;
EXCEPTION
    WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in function :- p_util_get_numeric_version(),parameter in_version not accept null or empty value. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
    WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack => l_format_call_stack, 
				in_error_code => SQLSTATE, 
				in_error_message => CONCAT(' Error in function :- p_util_get_numeric_version(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack => TRUE
			);
END;
$BODY$;