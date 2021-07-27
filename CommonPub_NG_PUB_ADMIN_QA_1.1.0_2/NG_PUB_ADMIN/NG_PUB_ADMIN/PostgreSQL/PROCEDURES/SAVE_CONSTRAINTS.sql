CREATE OR REPLACE PROCEDURE pub_admin.save_constraints
(
	in_cnstrnt_nm 				CHARACTER VARYING,
	in_cnstrnt_child_table_nm 	CHARACTER VARYING,
	in_cnstrnt_parent_table_nm 	CHARACTER VARYING,
	in_cnstrnt_schema_nm 		CHARACTER VARYING,
	in_constraint_def 			CHARACTER VARYING,
	in_constraint_type 			CHARACTER VARYING,
	in_user_id 					CHARACTER VARYING
)
LANGUAGE 'plpgsql'
AS $BODY$
/* ----------------------------------------------------------------------------------
      -- Author  : GF8561
      -- Created : 12/03/2021
      -- Purpose : This procedure is to save all foreign key constraints into pba_cnstrnt_t table.
	  --           This procedure is being called from HANDLE_CONSTRAINTS procedure.
      -----------------------------------------------------------------------------------
      -- Modification History
      -----------------------------------------------------------------------------------
      -- Ref#   Version#  Name                JIRA#    Date           Change Description
      -----------------------------------------------------------------------------------
      --  1.              Kalyan Kumar                12/03/2021      Initial Version
    ----------------------------------------------------------------------------------- */
DECLARE
    l_rcrd_count				SMALLINT;
	l_format_call_stack			TEXT;
	l_cnstrnt_nm				VARCHAR(100);
	l_cnstrnt_child_table_nm	VARCHAR(100);
	l_cnstrnt_parent_table_nm	VARCHAR(100);
	l_cnstrnt_schema_nm			VARCHAR(100);
	l_constraint_def			VARCHAR(100);
	l_constraint_type			VARCHAR(100);
	l_user_id					VARCHAR(100);

BEGIN
	l_cnstrnt_nm				:= UPPER(TRIM(in_cnstrnt_nm));
	l_cnstrnt_child_table_nm	:= UPPER(TRIM(in_cnstrnt_child_table_nm));
	l_cnstrnt_parent_table_nm	:= UPPER(TRIM(in_cnstrnt_parent_table_nm));
	l_cnstrnt_schema_nm			:= UPPER(TRIM(in_cnstrnt_schema_nm));
	l_constraint_def			:= UPPER(TRIM(in_constraint_def));
	l_constraint_type			:= UPPER(TRIM(in_constraint_type));
	l_user_id					:= UPPER(TRIM(in_user_id));

    IF l_cnstrnt_nm IS NOT NULL AND l_cnstrnt_child_table_nm IS NOT NULL AND l_cnstrnt_parent_table_nm IS NOT NULL AND l_cnstrnt_schema_nm IS NOT NULL 
	AND l_constraint_def IS NOT NULL AND l_constraint_type IS NOT NULL THEN
        SELECT COUNT(1) INTO l_rcrd_count
          FROM pub_admin.pba_cnstrnt_t a
         WHERE UPPER(a.cnstrnt_schema_nm) = l_cnstrnt_schema_nm
           AND UPPER(a.cnstrnt_parent_table_nm) = l_cnstrnt_parent_table_nm
           AND UPPER(a.cnstrnt_nm) = l_cnstrnt_nm
           AND UPPER(a.cnstrnt_child_table_nm) = l_cnstrnt_child_table_nm
		   AND UPPER(a.cnstrnt_type) = l_constraint_type;

        IF l_rcrd_count = 0 THEN
			RAISE INFO 'Inserted cons in pba_constrnt_t: %, %', l_constraint_def , l_cnstrnt_nm;
            INSERT INTO pub_admin.pba_cnstrnt_t(cnstrnt_nm, cnstrnt_schema_nm, cnstrnt_parent_table_nm, cnstrnt_child_table_nm, 
									  constraint_def, cnstrnt_type, rcrd_create_user_id, rcrd_create_ts, rcrd_updt_ts, rcrd_updt_user_id)
            VALUES(l_cnstrnt_nm, l_cnstrnt_schema_nm, l_cnstrnt_parent_table_nm, l_cnstrnt_child_table_nm, 
				   l_constraint_def, l_constraint_type, COALESCE(l_user_id, sbs_util.get_audit_user()), CURRENT_DATE, NULL, NULL);
		END IF;

    ELSIF l_cnstrnt_nm IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
    ELSIF l_cnstrnt_child_table_nm IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50002;
    ELSIF l_cnstrnt_parent_table_nm IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50003;
    ELSIF l_cnstrnt_schema_nm IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50004;
    ELSIF l_constraint_def IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50005;
	ELSIF l_constraint_type IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50006;
    END IF;

EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- save_constraints(), Constraint Name cannot have NULL or empty value', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- save_constraints(), Child Table Name cannot have NULL or empty value', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- save_constraints(), Parent Table Name cannot have NULL or empty value', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- save_constraints(), Schema Name cannot have NULL or empty value', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50005' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- save_constraints(), Constraint Defination cannot have NULL or empty value. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50006' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- save_constraints(), Constraint Type cannot have NULL or empty value.', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- save_constraints(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
