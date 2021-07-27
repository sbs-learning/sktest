CREATE OR REPLACE PROCEDURE pub_admin.handle_constraints
(
	in_schema_nm			CHARACTER VARYING,
	in_table_nm				CHARACTER VARYING,
	in_action				CHARACTER VARYING,
	in_user_id				CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING
)
LANGUAGE 'plpgsql'
AS $BODY$
/* ----------------------------------------------------------------------------------
      -- Author  : GF8561
      -- Created : 12/03/2021
      -- Purpose : This is the procedure Disabling and Enabling of Foreign key constraints from any table.
	  --           This is being called in Truncate table and Disable Constraints procedures.
      -----------------------------------------------------------------------------------
      -- Modification History
      -----------------------------------------------------------------------------------
      -- Ref#   Version#  Name                JIRA#    Date           Change Description
      -----------------------------------------------------------------------------------
      --  1.              Kalyan Kumar                12/03/2021      Initial Version
    ----------------------------------------------------------------------------------- */
DECLARE
	l_table_exists				VARCHAR(1);
    l_cnstrnt_nm				VARCHAR(200);
    l_cnstrnt_child_table_nm	VARCHAR(100);
	l_constraint_def			VARCHAR(200);
    l_query_str					VARCHAR(2000);
   	co_enable					CONSTANT VARCHAR(10) := 'ENABLE';
   	co_disable					CONSTANT VARCHAR(10) := 'DISABLE';
	co_handle_constraints		CONSTANT CHARACTER VARYING(100) DEFAULT 'HANDLE CONSTRAINTS';
	co_save_constraints			CONSTANT CHARACTER VARYING(100) DEFAULT 'SAVE CONSTRAINTS';
	co_drop_constraints			CONSTANT CHARACTER VARYING(100) DEFAULT 'DROP CONSTRAINTS';
	co_create_constraints		CONSTANT CHARACTER VARYING(100) DEFAULT 'CREATE CONSTRAINTS';

	l_schema_nm					CHARACTER VARYING(100);
	l_table_nm					CHARACTER VARYING(100);
	l_action					CHARACTER VARYING(20);
	l_user_id					CHARACTER VARYING(50);
    l_format_call_stack			TEXT;
      -- the Forein Keys info on Primary and Unique Keys of the given table
	c_table_info CURSOR
    FOR
	SELECT  DISTINCT(pgc.conname) AS constraint_name,
	        pg_get_constraintdef(pgc.oid, TRUE)::VARCHAR constraint_def,
		    ccu.table_schema AS schema_name,
			ccu.table_name AS parent_table,
			cls.relname AS child_table_name,
			ccu.column_name,
			contype AS constraint_type
	   FROM pg_constraint pgc
	   JOIN pg_namespace nsp ON nsp.oid = pgc.connamespace
	   JOIN pg_class  cls ON pgc.conrelid = cls.oid
	   LEFT JOIN information_schema.constraint_column_usage ccu
		 ON pgc.conname = ccu.constraint_name
		AND nsp.nspname = ccu.constraint_schema
	  WHERE contype in ('p','u','f')
		AND LOWER(ccu.table_name) = LOWER(in_table_nm)
		AND LOWER(ccu.table_schema) = LOWER(in_schema_nm)
		AND relpartbound IS NULL ORDER BY constraint_type;

BEGIN
	l_schema_nm		:= LOWER(TRIM(in_schema_nm));
	l_table_nm		:= LOWER(TRIM(in_table_nm));
	l_action		:= TRIM(in_action);
	l_user_id		:= TRIM(in_user_id);
	
    l_table_exists	:= 'N';
    l_query_str		:= NULL;
      -- Verify whether the table exists in the schema or not
    IF l_schema_nm IS NOT NULL AND l_table_nm IS NOT NULL
	THEN
		BEGIN
			SELECT 'Y'
			  INTO STRICT l_table_exists
			  FROM information_schema.tables
			 WHERE table_schema = l_schema_nm
			   AND table_name = l_table_nm;
		EXCEPTION
			WHEN NO_DATA_FOUND
			THEN
			   RAISE EXCEPTION USING errcode = 50001;
		END;
	ELSIF l_schema_nm IS NULL
    THEN
		RAISE EXCEPTION USING errcode = 50002;
	ELSIF l_table_nm IS NULL
    THEN
		RAISE EXCEPTION USING errcode = 50003;
    END IF;
	  
	IF l_action IS NULL
    THEN
		RAISE EXCEPTION USING errcode = 50004;
	END IF;

    IF l_action NOT IN (co_enable,co_disable)
    THEN
		RAISE EXCEPTION USING errcode = 50005;
	END IF;

        -- Disable Constraints
    IF(l_action = co_disable) THEN
		RAISE INFO 'Inside IF condition DISABLE Section';
        --<<disable_constr>>
        FOR l_table_info IN c_table_info
        LOOP
		 	
			RAISE INFO 'l_table_info.constraint_name - %',l_table_info.constraint_name;
			RAISE INFO 'l_table_info.child_table_name - %',l_table_info.child_table_name;
			RAISE INFO 'l_table_info.constraint_def - %',l_table_info.constraint_def;
            -- INSERT record in pba_cnstrnt_t table before "DISABLE"-ing a Constraint
            -- This gives us the information about disabled constraints while running this program

			CALL pub_admin.save_constraints 
				(
					in_cnstrnt_nm				=> l_table_info.constraint_name::VARCHAR,
					in_cnstrnt_child_table_nm	=> l_table_info.child_table_name::VARCHAR,
                    in_cnstrnt_parent_table_nm	=> l_table_nm,
                    in_cnstrnt_schema_nm		=> l_schema_nm,
                    in_user_id					=> l_user_id,
					in_constraint_type			=> l_table_info.constraint_type::VARCHAR,
                    in_constraint_def			=> l_table_info.constraint_def::VARCHAR
				);

			-- Loading the Query String with DISABLE CONSTRAINT DDL statement
            l_query_str :=
                  'ALTER TABLE '
               || in_schema_nm
               || '.'
               || l_table_info.child_table_name
			   || ' DROP CONSTRAINT '
               || l_table_info.constraint_name;
			
			RAISE INFO 'l_query_str - %', l_query_str;
			RAISE INFO 'Constraint disabled';
			
			CALL pub_admin.p_ctrl_log_event
				(
					in_event_constant			=> 'CO_HANDLE_CONSTRAINTS',
					in_table_name				=> l_table_nm,
					in_event_src_cd_location	=> 'HANDLE_CONSTRAINTS',
					in_event_statement			=> l_query_str,
					in_event_dtl				=> co_drop_constraints,
					in_user_id					=> l_user_id
				);
			BEGIN
				RAISE INFO 'Inside Begin statement to execute drop query';
				EXECUTE  l_query_str;
			EXCEPTION WHEN OTHERS THEN
				RAISE EXCEPTION USING errcode = 50006;
			END;
        END LOOP;
	END IF;

        -- Enable Foreign Key Constraints
        -- "VALIDATE" clause is used intentionally to raise an exception in case
        -- the child table is NOT EMPTY
    IF(l_action = co_enable) THEN
        --<<enable_constr>>
		CALL pub_admin.get_constraints 
			(
				in_cnstrnt_parent_table_nm   => l_table_nm,
				in_cnstrnt_schema_nm         => l_schema_nm,
				out_cnstrnt_nm               => l_cnstrnt_nm,
				out_cnstrnt_child_table_nm   => l_cnstrnt_child_table_nm,
				out_constraint_def           => l_constraint_def
			);
			
        WHILE l_cnstrnt_nm IS NOT NULL
        LOOP
            -- Loading the Query String with ENABLE CONSTRAINT DDL statement
            -- No need to qualify the Table Name with Schema Name as program is run as current_user
            l_query_str :=
                  'ALTER TABLE '
               || in_schema_nm
               || '.'
               || l_cnstrnt_child_table_nm
			   ||' ADD CONSTRAINT '
               || l_cnstrnt_nm
			   || ' '
			   || l_constraint_def;

			CALL pub_admin.p_ctrl_log_event
				(
					in_event_constant			=> 'CO_HANDLE_CONSTRAINTS',
					in_table_name				=> l_table_nm,
					in_event_src_cd_location	=> 'HANDLE_CONSTRAINTS',
					in_event_statement			=> l_query_str,
					in_event_dtl				=> co_create_constraints,
					in_user_id					=> l_user_id
				);
			BEGIN
				RAISE INFO '%',l_query_str;
			 	EXECUTE l_query_str;
			EXCEPTION WHEN OTHERS THEN
				RAISE EXCEPTION USING errcode = 50007;
			END;
			
            -- DELETE record from pba_cnstrnt_t table after "ENABLE"-ing a Constraint
		
			CALL pub_admin.delete_constraints 
				(
					in_cnstrnt_nm                => l_cnstrnt_nm,
					in_cnstrnt_child_table_nm    => l_cnstrnt_child_table_nm,
					in_cnstrnt_parent_table_nm   => l_table_nm,
					in_cnstrnt_schema_nm         => l_schema_nm
				);
            -- Keep getting the remaining Constraints Information from pba_cnstrnt_t to Enable them
			CALL pub_admin.get_constraints
				(
					in_cnstrnt_parent_table_nm   => l_table_nm,
					in_cnstrnt_schema_nm         => l_schema_nm,
					out_cnstrnt_nm               => l_cnstrnt_nm,
					out_cnstrnt_child_table_nm   => l_cnstrnt_child_table_nm,
					out_constraint_def           => l_constraint_def
				);
        END LOOP;
			RAISE INFO 'All Constraints Enabled';
         -- At the end of the processing if a record exists in this table means those constraints
         -- are still in "DISABLE" status
	END IF;
EXCEPTION	
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- handle_constraints(), table ', in_table_nm, ' does not exists.', in_schema_nm, CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- handle_constraints(), Schema Name cannot have null or Empty value.', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- handle_constraints(), Table Name cannot have null or Empty value.', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- handle_constraints(), Input value for Action on constarint is NULL, it should be ''ENABLE'' or ''DISABLE'' ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50005' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- handle_constraints(), Input value for Action on constarint is invalid it should be either ''ENABLE'' or ''DISABLE'' ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50006' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- handle_constraints(), Error While Disabling Constraint for Schema ', in_schema_nm, ' and Table ', in_table_nm, '.', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50007' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- handle_constraints(), Error While Enabling Constraint for Schema ', in_schema_nm, ' and Table ', in_table_nm, '.', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- handle_constraints(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
