CREATE OR REPLACE PROCEDURE sbs_util.grant_to_lt
(
	in_privilege		CHARACTER VARYING,
	in_object			CHARACTER VARYING,
	in_object_type		CHARACTER VARYING,
	in_grantable		CHARACTER VARYING DEFAULT 'NO'
)
/* --------------------------------------------------------------------------
   -- Purpose : Purpose of this Procedure is to provide grant on lt schema.
   -- Parameter Details
   --in_privilege character varying -- grant type,
   --in_object character varying -- object name on which grant to be provided by procedure
   --in_object_type character varying -- object type
   --in_grantable character varying DEFAULT 'NO' -- grant should be provided with grant option
   --------------------------------------------------------------------------------
   -- Name: grant_to_lt
   --------------------------------------------------------------------------------
   --
   -- Description:  grant_to_lt
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Sakshi Jain	 CSPUBCC-4303	03/16/2021	 grant_to_lt() initial draft
   --   2   Akshay       CSPUBCC-4533   06/03/2021   Handle empty string for input parameters.
*/
LANGUAGE 'plpgsql'
AS $$
DECLARE
	l_sql				VARCHAR(2000);
	l_related_users		VARCHAR(100);
	l_related_schema	VARCHAR(100);
	l_grantable			VARCHAR(3);
	l_object_type		VARCHAR(100);
	l_privilege			VARCHAR(1000);
	l_object			VARCHAR(100);
	stack				TEXT;
BEGIN
	l_privilege			:= UPPER(in_privilege);
	l_grantable			:= UPPER(in_grantable);
	l_object_type		:= UPPER(in_object_type);
	l_object			:= UPPER(in_object);
	
	IF (l_grantable NOT IN ('NO','YES')) THEN
		RAISE NOTICE 'Input in_grantable parameter is invalid it should be YES/NO';
		RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;
	
	IF ((l_grantable IS NULL) OR (l_grantable = '')) THEN
		RAISE NOTICE 'Input in_grantable parameter is invalid it should be YES/NO';
		RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;
	
	RAISE INFO '%',l_grantable;
	l_related_schema := sbs_util.get_lt_schema();
	RAISE INFO '%',l_related_schema;
	
	IF (l_related_schema IS NULL) THEN
		RAISE EXCEPTION USING ERRCODE = 50002;
	END IF;
	
	IF (l_privilege IS NULL OR l_privilege = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	ELSIF (l_object_type IS NULL OR l_object_type = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	ELSIF (l_object IS NULL OR l_object = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	END IF;
	
	l_related_users := sbs_util.get_schema_owner(l_related_schema);
	
	l_sql := sbs_util.get_sql_for_grant 
				(
					in_privilege			=> l_privilege,
					in_object_type			=> l_object_type,
					in_current_schema		=> CURRENT_SCHEMA::VARCHAR,
					in_object				=> l_object,
					in_related_users		=> l_related_users,
					in_grantable			=> l_grantable
				);
	RAISE INFO '%',l_sql;
	EXECUTE l_sql;

EXCEPTION
	WHEN null_value_not_allowed THEN
		GET STACKED DIAGNOSTICS stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- grant_to_lt(), Input in_privilege / in_object / in_object_type parameter is NULL or empty ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- grant_to_lt(), Input in_grantable parameter is invalid it should be YES/NO ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- grant_to_lt(), No related user found for lt ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- grant_to_lt() ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
  END;
$$;