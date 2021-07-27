CREATE OR REPLACE FUNCTION sbs_util.get_version_prefix
(
	in_version CHARACTER VARYING
)
RETURNS CHARACTER VARYING
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    -------------------------------------------------------------------------------------
    -- Name : get_version_prefix
    -------------------------------------------------------------------------------------
    -- Description : To find the prefix value of any version from it's version table
    -- Parameters : IN_VERSION
	----------------------------------------------------------------------------------
    -- RefNo Name            JIRA NO 		   Date           Description of change
    -- ----- ---------------- -------- ---------------------------------------------
    -- 	1	Akshay  	    CSPUBCC-4401	04/19/2021     	  Added exception block in the query.
	--  2   Akshay          CSPUBCC-4533    06/04/2021        Handle empty string for input parameters.
    -------------------------------------------------------------------------------------
      l_version_prefix		VARCHAR(200);
	  l_version				VARCHAR(200);
	  l_format_call_stack	TEXT;
BEGIN
	l_version := TRIM(in_version);
	l_version_prefix := l_version;
	
    IF TRIM(BOTH ' ' FROM l_version_prefix) IS NULL OR l_version_prefix = '' THEN
    	RAISE INFO 'Invalid input version passed <%>',in_version;
		RAISE EXCEPTION USING errcode = 50001;
	END IF;
    
	l_version_prefix := SUBSTRING(l_version,'[^[:digit:]_|-]*');
    RETURN l_version_prefix;
EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in function :- get_version_prefix(),parameter in_version not accept null or empty value. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN 
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in function :- get_version_prefix(), ', CHR(10), ' SQLERRM:- ',SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;