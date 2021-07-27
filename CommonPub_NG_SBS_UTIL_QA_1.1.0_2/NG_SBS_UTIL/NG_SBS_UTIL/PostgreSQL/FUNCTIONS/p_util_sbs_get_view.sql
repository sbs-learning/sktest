CREATE OR REPLACE FUNCTION sbs_util.p_util_sbs_get_view
(
	in_fq_view_name CHARACTER VARYING   -- in_fq_view_name parameter should have schema_name.view_name
)
    RETURNS TEXT
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
/* 
   -----------------------------------------------------------------------------------------------
   -- Name: p_util_sbs_get_view
   -----------------------------------------------------------------------------------------------
   --
   -- Description: Purpose of this function is to fetch the view script from sbs_view_script table 
                   against the fully qualified view name that is provided in parameter value.
   --
   ------------------------------------------------------------------------------------------------
   -- RefNo Name             JIRA NO 		 Date         Description of change
   -- ----- ---------------- -------- ------------------ ------------------------------------------
   -- 	1	Akshay	        CSPUBCC-4506	6/10/21        p_util_sbs_get_view() initial draft
*/
    l_fq_view_name         VARCHAR(200);
	l_fq_view_name_exits   SMALLINT;
	l_vw_script            TEXT;
	l_format_call_stack    TEXT;

BEGIN

	l_fq_view_name := LOWER(TRIM(in_fq_view_name));
	
    IF (l_fq_view_name IS NULL OR l_fq_view_name = '') THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
    END IF;
	
	SELECT COUNT(1) INTO l_fq_view_name_exits FROM sbs_util.sbs_view_script WHERE FQ_VW_NAME = l_fq_view_name;
	
	IF (l_fq_view_name_exits = 1) THEN
		SELECT vw_script INTO l_vw_script FROM sbs_util.sbs_view_script WHERE FQ_VW_NAME = l_fq_view_name;
	ELSE
		RAISE EXCEPTION USING ERRCODE = 50002;
	END IF;
	
    RETURN l_vw_script;

EXCEPTION
	WHEN SQLSTATE '50001' THEN  
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in function :- p_util_sbs_get_view(), input parameter in_fq_view_name cannot be null or empty' , CHR(10), 'SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in function :- p_util_sbs_get_view(),parameter ', in_fq_view_name, ' not found in table sbs_view_script. ', CHR(10), ' SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in function :- p_util_sbs_get_view()', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
