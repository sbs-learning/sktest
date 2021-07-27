CREATE OR REPLACE PROCEDURE sbs_util.get_all_versions()
LANGUAGE 'plpgsql'
SECURITY DEFINER
AS $BODY$
DECLARE
   /* --------------------------------------------------------------------------
   --
   --------------------------------------------------------------------------------
   -- Name: get_all_versions
   --------------------------------------------------------------------------------
   --
   -- Description:  Purpose of this procedure is to get the versions of all tools from their version table
   --               from all schemas of that stack.
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Kalyan	     CSPUBCC-4401	04/20/2021	get_all_versions() initial draft
   */

   	l_ver  				VARCHAR(4000);
	l_tool_name 		VARCHAR(100);
	l_version   		VARCHAR(100);
	tbl    				RECORD;
	l_format_call_stack TEXT;
BEGIN

	FOR tbl
	   IN (SELECT schemaname schema_name, tablename table_name
			 FROM pg_tables
			WHERE UPPER(tablename) LIKE '%_VERSION'
			  AND schemaname NOT IN ('pg_catalog')
		  )
	LOOP
    	l_ver := sbs_util.get_version 
			(	
				in_schema_name          	=> tbl.schema_name::varchar,
				in_version_table_name   	=> tbl.table_name::varchar
			);
		RAISE INFO '%.% - % ',tbl.schema_name, tbl.table_name, l_ver;
	END LOOP;
EXCEPTION
    WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in Procedure :- get_all_versions(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;