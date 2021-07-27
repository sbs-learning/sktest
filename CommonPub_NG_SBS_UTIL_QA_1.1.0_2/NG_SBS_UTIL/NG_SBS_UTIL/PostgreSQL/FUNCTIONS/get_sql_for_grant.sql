CREATE OR REPLACE FUNCTION sbs_util.get_sql_for_grant
(
	in_privilege		CHARACTER VARYING,
	in_object_type		CHARACTER VARYING,
	in_current_schema   CHARACTER VARYING,
	in_object			CHARACTER VARYING,
	in_related_users    CHARACTER VARYING,
	in_grantable		CHARACTER VARYING DEFAULT 'NO'
)
RETURNS VARCHAR AS
$$
/*--------------------------------------------------------------------------
   -- Purpose : Purpose of this function is to prepare grant statement to all kinds of database objects.
   --------------------------------------------------------------------------------
   -- Name: get_sql_for_grant
   --------------------------------------------------------------------------------
   --
   -- Description:  get_sql_for_grant
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Akshay        CSPUBCC-4571	06/08/2021	 get_sql_for_grant() initial draft
*/
DECLARE
	l_sql				VARCHAR(2000);
	l_related_users		VARCHAR(100);
	l_grantable			VARCHAR(3);
	l_object_type		VARCHAR(100);
	l_current_schema    VARCHAR(100);
	l_privilege			VARCHAR(1000);
	l_object			VARCHAR(100);
	stack				TEXT;
BEGIN
	l_privilege			:= UPPER(in_privilege);
	l_object_type		:= UPPER(in_object_type);
	l_current_schema    := LOWER(in_current_schema);
	l_object			:= LOWER(in_object);
	l_related_users     := LOWER(in_related_users);
	l_grantable			:= UPPER(in_grantable);
	
	IF (l_grantable NOT IN ('NO','YES')) OR ((l_grantable IS NULL) OR (l_grantable = '')) THEN
		RAISE NOTICE 'Input in_grantable parameter is invalid it should be YES/NO';
		RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;
	
	RAISE INFO 'l_grantable: % ',l_grantable;
	
	IF (l_privilege IS NULL OR l_privilege = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	ELSIF (l_object_type IS NULL OR l_object_type = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	ELSIF (l_current_schema IS NULL OR l_current_schema = '') THEN
		RAISE EXCEPTION null_value_not_allowed;	
	ELSIF (l_object IS NULL OR l_object = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	ELSIF (l_related_users IS NULL OR l_related_users = '') THEN
		RAISE EXCEPTION null_value_not_allowed;
	END IF;
	
	IF l_object_type IN ('TABLE','PROCEDURE','FUNCTION','SEQUENCE') THEN
	    l_sql := CONCAT ('GRANT ' , l_privilege , ' ON ' , l_object_type , ' ' , l_current_schema , '.' ,
			 l_object , ' TO ' , l_related_users , CASE
			   WHEN l_grantable = 'YES' THEN
				' WITH GRANT OPTION'
			   ELSE
				' '
			 END
			 );
	ELSIF l_object_type IN ('VIEW','MATERIALIZED VIEW') THEN
	    l_sql := CONCAT ('GRANT ' , l_privilege , ' ON ' , ' ' , l_current_schema , '.' ,
			 l_object , ' TO ' , l_related_users , CASE
			   WHEN l_grantable = 'YES' THEN
				' WITH GRANT OPTION'
			   ELSE
				' '
			 END
			 );
	END IF;
	
	RETURN l_sql;
EXCEPTION
    WHEN null_value_not_allowed THEN
		GET STACKED DIAGNOSTICS stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_sql_for_grant(), Input in_privilege / in_object_type / in_current_schema / in_object / in_related_users parameter is NULL or empty ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_sql_for_grant(), Input in_grantable parameter is invalid it should be YES/NO ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_sql_for_grant() ',CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$$ LANGUAGE plpgsql;