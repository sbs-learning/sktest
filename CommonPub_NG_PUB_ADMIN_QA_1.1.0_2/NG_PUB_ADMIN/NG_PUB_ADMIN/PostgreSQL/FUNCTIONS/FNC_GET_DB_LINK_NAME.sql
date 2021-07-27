CREATE OR REPLACE FUNCTION pub_admin.fnc_get_db_link_name()
    RETURNS character varying
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
   /* ----------------------------------------------------------------------------------
      -- Author  : GF8561
      -- Created : 01/03/2021
      -- Purpose : This is the function to get the name of DBLink that will be created.
      -----------------------------------------------------------------------------------
      -- Modification History
      -----------------------------------------------------------------------------------
      -- Ref#   Version#  Name                JIRA#    Date           Change Description
      -----------------------------------------------------------------------------------
      --  1.              Kalyan Kumar                01/03/2021      Initial Version
    ----------------------------------------------------------------------------------- */
DECLARE
	l_db_link_name 		VARCHAR(200);
	l_format_call_stack TEXT;
BEGIN
	l_db_link_name := 'DBL_' || current_database();
	
	RETURN l_db_link_name;
EXCEPTION
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in function :- fnc_get_db_link_name(), ' , CHR(10), 'SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;