CREATE OR REPLACE FUNCTION sbs_util.p_util_get_version
(
	in_schema_name 			CHARACTER VARYING,
	in_version_table_name 	CHARACTER VARYING
)
RETURNS CHARACTER VARYING
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    -------------------------------------------------------------------------------------
    -- Name : p_util_get_version
    -------------------------------------------------------------------------------------
    -- Description : This is the wrapper procedure for get_version.
    -- Parameters : in_schema_name, in_version_table_name.
	----------------------------------------------------------------------------------
    -- RefNo Name            JIRA NO 		   Date           Description of change
    -- ----- ---------------- -------- ---------------------------------------------
    -- 	1	Akshay  	    CSPUBCC-4401	04/19/2021     	  Added exception block in the query.
	--  2   Akshay          CSPUBCC-4533    06/04/2021        Handle empty string for input parameters.
    -------------------------------------------------------------------------------------
    l_out_version         VARCHAR(2000);
	l_schema_name         VARCHAR(200);
	l_version_table_name  VARCHAR(200);
    l_format_call_stack   TEXT;    
	l_error_msg           VARCHAR(2000);	
BEGIN

	l_schema_name 			:= TRIM(LOWER(in_schema_name));
	l_version_table_name 	:= TRIM(LOWER(in_version_table_name));
	
	IF l_schema_name = '' OR l_version_table_name = '' THEN
	   RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;

	IF l_schema_name IS NOT NULL AND l_version_table_name IS NOT NULL THEN
			
		l_out_version :=
			sbs_util.get_version 
				(
					in_schema_name          => l_schema_name,
					in_version_table_name   => l_version_table_name
				);
	ELSE 
		IF  l_schema_name IS NULL THEN
        	l_error_msg := 'Schema name not provided';
        	l_out_version := l_error_msg;
		END IF;
		IF  l_version_table_name IS NULL THEN
        	l_error_msg := 'Version table name not provided';
			l_out_version := l_error_msg;
		END IF;
	END IF;
	RETURN l_out_version;
EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in function :- p_util_get_version(), input parameter in_schema_name or in_version_table_name cannot be empty. ', CHR(10), ' SQLERRM :-  ', SQLERRM),
				in_show_stack 		=> TRUE
			);
	WHEN OTHERS THEN
	   GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in function :- p_util_get_version(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;