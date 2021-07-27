CREATE OR REPLACE PROCEDURE sbs_util.set_sequence_from_column
(
	in_schema_name		CHARACTER VARYING,
	in_table_name		CHARACTER VARYING,
	in_column_name		CHARACTER VARYING,
	in_sequence_name	CHARACTER VARYING,
	in_echo_sql			BOOLEAN DEFAULT FALSE
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE

   -----------------------------------------------------------------------------
   -- Name :set_sequence_from_column                                             
   -----------------------------------------------------------------------------
   -- Description : It is use to call the procedure set_sequence_nextval.
   -- Parameters: in_schema_name, in_table_name, in_column_name, in_sequence_name, in_echo_sql
   ------------------------------------------------------------------------------
   -- RefNo Name             JIRA NO       Date     Description of change                      
   -- ----- ---------------- -------- ------------ -------------------------------
   -- 1     Akshay       CSPUBCC-4312   04/05/2021  Initial Version                         
   -- 2	    Akshay	     CSPUBCC-4383	04/14/2021	Added variable l_schema_name, l_table_name, l_sequence_name,
   --                                               l_column_name and lower function in query
   -- 3     Akshay       CSPUBCC-4533   06/03/2021  Handle empty string for input parameters.   
   ------------------------------------------------------------------------------

    l_max_column_value		NUMERIC;
    l_sql					VARCHAR(500);
	l_schema_name			VARCHAR(100);
	l_table_name			VARCHAR(100);
	l_column_name			VARCHAR(100);
	l_sequence_name			VARCHAR(100);
	l_seq_exists         	VARCHAR(1);
	l_col_exists			VARCHAR(1);
	l_format_call_stack		Text;

BEGIN

    l_schema_name   := LOWER(TRIM(in_schema_name));
	l_table_name    := LOWER(TRIM(in_table_name));
	l_column_name   := LOWER(TRIM(in_column_name));
	l_sequence_name := LOWER(TRIM(in_sequence_name));

	IF l_schema_name IS NULL OR l_schema_name = '' OR l_table_name IS NULL OR l_table_name = '' OR l_column_name IS NULL OR l_column_name = '' 
	    OR l_sequence_name IS NULL OR l_sequence_name = '' THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;

	BEGIN
		SELECT 'Y' into strict l_col_exists
		  FROM information_schema.columns
		 where table_name = l_table_name::name
		   and column_name = l_column_name::name
		   and table_schema = l_schema_name::name;
	EXCEPTION
		WHEN NO_DATA_FOUND THEN
			RAISE EXCEPTION USING ERRCODE = 50002;
	END;

	IF l_col_exists = 'Y' THEN
		BEGIN
			SELECT 'Y'
			  INTO STRICT l_seq_exists
			  FROM pg_catalog.pg_sequences
			 WHERE schemaname = l_schema_name::name
			   AND sequencename = l_sequence_name::name;
		EXCEPTION
			WHEN NO_DATA_FOUND THEN
				RAISE EXCEPTION USING ERRCODE = 50003;
		END;
	END IF;

    l_sql := 'SELECT MAX('||l_column_name||') FROM '||l_schema_name||'.'||l_table_name;
    IF in_echo_sql THEN
		RAISE INFO 'Executing: %', l_sql;
    END IF;

    EXECUTE l_sql INTO l_max_column_value;

	IF in_echo_sql THEN
		RAISE INFO 'Result: %', l_max_column_value;
    END IF;

    CALL sbs_util.set_sequence_nextval
		(
			in_schema_name		=> l_schema_name,
			in_sequence_name	=> l_sequence_name,
			in_target_value		=> COALESCE(l_max_column_value,0)+1,
			in_echo_sql			=> in_echo_sql
		);
EXCEPTION
	WHEN SQLSTATE '50001' THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
	  	CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- set_sequence_from_column(), Either of the input parameters is having NULL or Empty value. ', ' in_schema_name - ', in_schema_name, ', in_sequence_name - ', in_sequence_name, ', in_table_name - ', in_table_name,', in_column_name - ', in_column_name, CHR(10) , 'SQLERRM:- ' , SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
	  	CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- set_sequence_from_column(), Either Schema name or Table name or Column name does not exists. ', ' in_schema_name - ', in_schema_name, ', in_table_name - ', in_table_name,', in_column_name - ', in_column_name, CHR(10) , 'SQLERRM:- ' , SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
	  	CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- set_sequence_from_column(), Sequence name does not exists. ', ' in_sequence_name - ', in_sequence_name, CHR(10) , 'SQLERRM:- ' , SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- set_sequence_from_column(). ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;