CREATE OR REPLACE PROCEDURE pub_admin.delete_constraints
(
	in_cnstrnt_nm				CHARACTER VARYING,
	in_cnstrnt_child_table_nm	CHARACTER VARYING,
	in_cnstrnt_parent_table_nm	CHARACTER VARYING,
	in_cnstrnt_schema_nm		CHARACTER VARYING
)
LANGUAGE 'plpgsql'
AS $BODY$
/* ----------------------------------------------------------------------------------
      -- Author  : GF8561
      -- Created : 12/03/2021
      -- Purpose : This procedure is to delete foreign key constraints from pba_cnstrnt_t table.
	  --           That were saved by save_constraints procedures. These Constraints are getting Enabled and then getting deleted.
	  --           This procedure is being called from HANDLE_CONSTRAINTS procedure.
	  -----------------------------------------------------------------------------------
      -- Modification History
      -----------------------------------------------------------------------------------
      -- Ref#   Version#  Name                JIRA#    Date           Change Description
      -----------------------------------------------------------------------------------
      --  1.              Kalyan Kumar                12/03/2021      Initial Version
    ----------------------------------------------------------------------------------- */
DECLARE
	l_format_call_stack			TEXT;
	l_cnstrnt_nm				VARCHAR(200);
	l_cnstrnt_child_table_nm	VARCHAR(200);
	l_cnstrnt_parent_table_nm	VARCHAR(200);
	l_cnstrnt_schema_nm			VARCHAR(200);
BEGIN

	l_cnstrnt_nm				:= UPPER(TRIM(in_cnstrnt_nm));
	l_cnstrnt_child_table_nm	:= UPPER(TRIM(in_cnstrnt_child_table_nm));
	l_cnstrnt_parent_table_nm	:= UPPER(TRIM(in_cnstrnt_parent_table_nm));
	l_cnstrnt_schema_nm			:= UPPER(TRIM(in_cnstrnt_schema_nm));

    IF l_cnstrnt_nm IS NOT NULL AND l_cnstrnt_child_table_nm IS NOT NULL AND l_cnstrnt_parent_table_nm IS NOT NULL AND l_cnstrnt_schema_nm IS NOT NULL THEN
        DELETE FROM pub_admin.pba_cnstrnt_t a
         WHERE UPPER(a.cnstrnt_schema_nm) = l_cnstrnt_schema_nm
           AND UPPER(a.cnstrnt_parent_table_nm) = l_cnstrnt_parent_table_nm
           AND UPPER(a.cnstrnt_nm) = l_cnstrnt_nm
           AND UPPER(a.cnstrnt_child_table_nm) = l_cnstrnt_child_table_nm;
    ELSIF l_cnstrnt_nm IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
    ELSIF l_cnstrnt_child_table_nm IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50002;
    ELSIF l_cnstrnt_parent_table_nm IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50003;
    ELSIF l_cnstrnt_schema_nm IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50004;
    END IF;

EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- delete_constraints(), Constraint Name cannot have NULL or empty value.', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- delete_constraints(), Child table Name cannot have NULL or empty value.', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- delete_constraints(), Parent Table Name cannot have NULL or empty value.', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- delete_constraints(), Schema Name cannot have NULL or empty value.', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- delete_constraints(), ' , CHR(10), ' SQLERRM :- ',  SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
