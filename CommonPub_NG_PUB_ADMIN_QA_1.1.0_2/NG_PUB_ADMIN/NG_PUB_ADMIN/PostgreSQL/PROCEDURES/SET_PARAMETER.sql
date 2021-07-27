CREATE OR REPLACE PROCEDURE pub_admin.set_parameter
(
	in_param_nm 		CHARACTER VARYING,
	in_param_value_txt 	CHARACTER VARYING,
	in_param_dsc 		CHARACTER VARYING
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    /* 
	--------------------------------------------------------------------------------
    -- Name: set_Parameter
    --------------------------------------------------------------------------------
    --
    -- Description: Purpose of this Procedure is to insert the data into the main table 
	--           and to update the data if param_nm equal to in_param_nm.
    -- Parameters: in_param_nm, in_param_value_txt, in_param_dsc
    --
    --------------------------------------------------------------------------------
    -- RefNo Name            JIRA NO 		Date     Description of change
    --------------------------------------------------------------------------------
    -- 	1	Akshay	        CSPUBCC-4305		     set_Parameter() initial draft
	*/
	l_format_call_stack TEXT;
	l_param_nm 			VARCHAR(2000);
	l_param_value_txt 	VARCHAR(2000);
	l_param_dsc 		VARCHAR(2000);
BEGIN

	l_param_nm 			:= TRIM(in_param_nm);
	l_param_value_txt 	:= TRIM(in_param_value_txt);
	l_param_dsc 		:= TRIM(in_param_dsc);

    IF (l_param_nm IS NULL OR l_param_nm = '') THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
    END IF;

    IF (l_param_value_txt IS null OR l_param_value_txt = '') THEN
		RAISE EXCEPTION USING ERRCODE = 50002;
    END IF;

    IF (l_param_dsc IS null OR l_param_dsc = '') THEN
	   RAISE EXCEPTION USING ERRCODE = 50003;
    END IF;

	INSERT INTO pub_admin.PBA_PARAM_T (PARAM_NM, PARAM_VALUE_TXT, PARAM_DSC, RCRD_CREATE_USER_ID)
	VALUES (l_param_nm, l_param_value_txt, l_param_dsc, CURRENT_USER)
	ON CONFLICT (PARAM_NM)
	DO UPDATE SET PARAM_VALUE_TXT = l_param_value_txt, PARAM_DSC = l_param_dsc;

EXCEPTION
    WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- set_Parameter(),parameter in_param_nm cannot have null or empty value', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
    WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- set_Parameter(),parameter in_param_value_txt cannot have null or empty value.', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
    WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		 	(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE,
			 	in_error_message	=> CONCAT('Error in procedure :- set_Parameter(),parameter in_param_dsc cannot have null or empty value.', CHR(10), ' SQLERRM :- ', SQLERRM),
			 	in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- set_Parameter() ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;