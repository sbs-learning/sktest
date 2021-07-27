CREATE OR REPLACE PROCEDURE sbs_util.drop_column
(
	IN_TABNAME CHARACTER VARYING, 
	IN_COLNAME CHARACTER VARYING
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   -----------------------------------------------------------------------------
   -- Name :DROP_COLUMN                                             
   -----------------------------------------------------------------------------
   -- Description : To drop column of a particular table.
   -- Parameters: IN_TABNAME, IN_COLNAME
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Akshay	     CSPUBCC-4313	04/05/2021  DROP_COLUMN() initial draft
   -- 	2	Akshay	     CSPUBCC-4383	04/14/2021	Added variables l_tabname, l_colname 
   --                                               and lower function in query.
   --   3   Akshay       CSPUBCC-4533   06/03/2021  Handle empty string for input parameters.

   l_sql                VARCHAR(32767);
   l_tabname            VARCHAR(100);
   l_colname            VARCHAR(100);
   l_col_exists			VARCHAR(1);
   l_tot_col_count		SMALLINT;
   l_format_call_stack  TEXT;

BEGIN

    l_tabname := LOWER(TRIM(IN_TABNAME));
	l_colname := LOWER(TRIM(IN_COLNAME));

	IF l_tabname IS NULL OR l_colname IS NULL OR l_tabname = '' OR l_colname = '' THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;

	SELECT 'Y' INTO l_col_exists
	FROM information_schema.columns
	WHERE TABLE_NAME = l_tabname
	  AND column_name = l_colname;
	  
	IF l_col_exists = 'Y' THEN
		SELECT COUNT(1) INTO STRICT l_tot_col_count
		  FROM information_schema.columns
	     WHERE TABLE_NAME = l_tabname;

		IF l_tot_col_count > 1 THEN
			RAISE INFO 'More than one column exists in table ';
		ELSIF l_tot_col_count = 1 THEN
			RAISE EXCEPTION USING ERRCODE = 50002;
		END IF;
	ELSE 
		RAISE EXCEPTION USING ERRCODE = 50003;
	END IF;


	l_sql:='Alter table '||l_tabname||' drop column '||l_colname;
	RAISE INFO 'Executing: %',l_sql;
    EXECUTE l_sql;

EXCEPTION
	WHEN SQLSTATE '42P16' THEN	   
		RAISE INFO 'Trying below SQL statement, direct column drop is not supported for table having partition on column: %',IN_COLNAME;
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
	           	in_error_code		=> SQLSTATE, 
	           	in_error_message	=> CONCAT(' Error in procedure :- drop_column(). ', CHR(10), ' SQLERRM:- ', SQLERRM),
	           	in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
	           	in_error_code		=> SQLSTATE, 
	           	in_error_message	=> CONCAT(' Error in procedure :- drop_column(). ',' NULL or Empty value is passed for the Input parameters. ', CHR(10), ' SQLERRM:- ', SQLERRM),
	           	in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
	           	in_error_code		=> SQLSTATE, 
	           	in_error_message	=> CONCAT(' Error in procedure :- drop_column(). ',' Cannot drop column - ', l_colname, ', Only one column exists in Table. ', CHR(10), ' SQLERRM:- ', SQLERRM),
	           	in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
	           	in_error_code		=> SQLSTATE, 
	           	in_error_message	=> CONCAT(' Error in procedure :- drop_column(). ',' Either table or column does not exists ', CHR(10), ' SQLERRM:- ', SQLERRM),
	           	in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_column(). ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;