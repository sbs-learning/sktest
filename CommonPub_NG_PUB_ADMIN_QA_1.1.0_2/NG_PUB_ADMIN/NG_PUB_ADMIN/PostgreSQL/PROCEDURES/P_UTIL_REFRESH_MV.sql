CREATE OR REPLACE PROCEDURE p_util_refresh_mv
(
	in_schema_nm 		CHARACTER VARYING,
	in_mv_nm 			CHARACTER VARYING,
	with_data_flg 		CHARACTER VARYING DEFAULT 'Y'::CHARACTER VARYING
)
LANGUAGE 'plpgsql'
SECURITY DEFINER
AS $BODY$
	-----------------------------------------------------------------------------
   -- Name : P_UTIL_REFRESH_MV
   -----------------------------------------------------------------------------
   -- Description : This is a wrapper procedure to call refresh mv of PubAdmin.
   -- Parameters: IN_SCHEMA_NM, IN_MVIEW_NM, WITH_DATA_FLG
   ------------------------------------------------------------------------------
   -- RefNo  Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- -------------------------------------------
   -- 1      Kalyan Kumar   CSPUBCC-4479   04/30/2021  Initial Draft
   ------------------------------------------------------------------------------
DECLARE
	l_format_call_stack	TEXT;
	l_schema_nm 		CHARACTER VARYING(100);
	l_mv_nm 			CHARACTER VARYING(100);
	l_with_data_flg		CHARACTER VARYING(1);
BEGIN
	l_schema_nm			:= LOWER(TRIM(in_schema_nm));
	l_mv_nm				:= LOWER(TRIM(in_mv_nm));
	l_with_data_flg		:= UPPER(TRIM(with_data_flg));

	CALL pub_admin.refresh_mv 
		(
			in_schema_nm     		=> l_schema_nm,
			in_mv_nm 				=> l_mv_nm,
			with_data_flg 			=> l_with_data_flg
		);
EXCEPTION
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- refresh_mv(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
