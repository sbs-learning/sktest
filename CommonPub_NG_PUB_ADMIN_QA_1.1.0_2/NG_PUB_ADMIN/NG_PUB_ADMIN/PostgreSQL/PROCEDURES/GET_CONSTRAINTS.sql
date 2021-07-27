CREATE OR REPLACE PROCEDURE pub_admin.get_constraints
(
	in_cnstrnt_parent_table_nm 			CHARACTER VARYING,
	in_cnstrnt_schema_nm 				CHARACTER VARYING,
	INOUT out_cnstrnt_nm 				CHARACTER VARYING,
	INOUT out_cnstrnt_child_table_nm 	CHARACTER VARYING,
	INOUT out_constraint_def	 		CHARACTER VARYING
)
LANGUAGE 'plpgsql'
AS $BODY$
/* ----------------------------------------------------------------------------------
      -- Author  : GF8561
      -- Created : 12/03/2021
      -- Purpose : This procedure is to get all foreign key constraints that were saved by save_constraints procedures.
	  --           This procedure is being called from HANDLE_CONSTRAINTS procedure.
      -----------------------------------------------------------------------------------
      -- Modification History
      -----------------------------------------------------------------------------------
      -- Ref#   Version#  Name                JIRA#    Date           Change Description
      -----------------------------------------------------------------------------------
      --  1.              Kalyan Kumar                12/03/2021      Initial Version
    ----------------------------------------------------------------------------------- */
DECLARE
	l_format_call_stack 		TEXT;
	l_cnstrnt_parent_table_nm 	VARCHAR(200);
	l_cnstrnt_schema_nm  		VARCHAR(200);
BEGIN

	l_cnstrnt_parent_table_nm 	:= UPPER(TRIM(in_cnstrnt_parent_table_nm));
	l_cnstrnt_schema_nm 		:= UPPER(TRIM(in_cnstrnt_schema_nm));
	
    IF l_cnstrnt_parent_table_nm IS NOT NULL AND l_cnstrnt_schema_nm IS NOT NULL THEN
        BEGIN
            SELECT c.cnstrnt_nm, c.cnstrnt_child_table_nm, c.constraint_def
              INTO STRICT out_cnstrnt_nm, out_cnstrnt_child_table_nm, out_constraint_def
              FROM pub_admin.pba_cnstrnt_t c
             WHERE UPPER(c.cnstrnt_schema_nm) = UPPER(l_cnstrnt_schema_nm)
               AND UPPER(c.cnstrnt_parent_table_nm) = UPPER(l_cnstrnt_parent_table_nm)
			   Order by cnstrnt_type desc
			   LIMIT 1;
			   RAISE INFO 'Get cons from pba_cons_t: %, %', out_constraint_def , out_cnstrnt_nm;
        EXCEPTION WHEN no_data_found THEN
            out_cnstrnt_nm             := NULL;
            out_cnstrnt_child_table_nm := NULL;
            out_constraint_def         := NULL;
            NULL;
        END;
    ELSIF l_cnstrnt_parent_table_nm IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
    ELSIF l_cnstrnt_schema_nm IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50002;
    END IF;

EXCEPTION WHEN OTHERS THEN
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- get_constraints(), Table Name cannot have NULL or empty value. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- get_constraints(), Schema Name cannot have NULL or empty value', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- get_constraints(), ' , CHR(10), ' SQLERRM :- ',  SQLERRM),
				in_show_stack		=> TRUE
			);

END;
$BODY$;
