CREATE OR REPLACE FUNCTION sbs_util.get_version
(
	in_schema_name 			CHARACTER VARYING,
	in_version_table_name 	CHARACTER VARYING
)
RETURNS CHARACTER VARYING
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    -------------------------------------------------------------------------------------
    -- Name : get_version
    -------------------------------------------------------------------------------------
    -- Description : To get version from version table.
    -- Parameters : in_schema_name, in_version_table_name
	----------------------------------------------------------------------------------
    -- RefNo Name            JIRA NO 		   Date           Description of change
    -- ----- ---------------- -------- ---------------------------------------------
    -- 	1	Akshay  	    CSPUBCC-4401	04/19/2021     	  Added exception block in the query.
    -------------------------------------------------------------------------------------
	l_ver_tab_exist       SMALLINT;
	l_ver_col_exist       SMALLINT;
    l_out_version         VARCHAR(2000);
    l_sql_txt             VARCHAR(4000);
    l_ver_exist		      SMALLINT;
	l_schema_name         VARCHAR(500);
	l_version_table_name  VARCHAR(500);
    l_error_msg           VARCHAR(2000);
    l_format_call_stack   TEXT;    
	
BEGIN

	l_schema_name 			:= TRIM(LOWER(in_schema_name));
	l_version_table_name 	:= TRIM(LOWER(in_version_table_name));
	
	IF l_schema_name IS NOT NULL AND l_version_table_name IS NOT NULL THEN
    	SELECT COUNT(1) INTO l_ver_tab_exist
		  FROM INFORMATION_SCHEMA.TABLES
		 WHERE table_name = l_version_table_name
           AND table_schema = l_schema_name;
	
		IF l_ver_tab_exist = 1 THEN
			--As we are using COUNT(*) , this query will return ONLY one row so no need to handle NO_DATA_FOUND , TOO_MANY_ROWS
			--Determine if the version table contains IS_CURRENT column
			SELECT COUNT(1) INTO strict l_ver_col_exist
			  FROM information_schema.columns
			 WHERE table_name = l_version_table_name
			   AND table_schema = l_schema_name
			   AND column_name = 'is_current';

			-- if the version table contains IS_CURRENT columns
			IF l_ver_col_exist = 1 THEN
				--Check if version table contains any data
					l_sql_txt:='SELECT COUNT(1) FROM '||l_schema_name||'.'||l_version_table_name||' WHERE IS_CURRENT=1';
				EXECUTE l_sql_txt INTO l_ver_exist;
				--if data exists in the version table , fetch the version
				IF l_ver_exist=1 THEN
					l_sql_txt := 'SELECT VERSION  FROM '|| l_schema_name||'.'||l_version_table_name ||' WHERE IS_CURRENT=1';
					EXECUTE l_sql_txt INTO l_out_version;
					--if there is no data in the version table
				ELSE
					l_out_version := 'No data found in version table '||l_schema_name||'.'||l_version_table_name||', column IS_CURRENT is not set to 1.';
				END IF;
			ELSE
				SELECT COUNT(1) INTO l_ver_col_exist
				  FROM information_schema.columns
				 WHERE TABLE_NAME = l_version_table_name
				   AND table_schema = l_schema_name
				   AND COLUMN_NAME = 'applied_date';

			-- if the version table does not have IS_CURRENT columns , fetch the version using latest release_date
			--As we are using rownum=1 , this query will return ONLY one row so no need to handle NO_DATA_FOUND , TOO_MANY_ROWS

				IF l_ver_col_exist = 1 THEN
			--Check if version table contains any data
					l_sql_txt := 'SELECT COUNT(1) from '|| '(SELECT VERSION FROM '||l_schema_name||'.'||l_version_table_name ||' order by APPLIED_DATE desc) a
								 limit 1';
					EXECUTE l_sql_txt INTO l_ver_exist;
					IF  l_ver_exist = 1 THEN
						l_sql_txt := 'SELECT VERSION from '||'(SELECT VERSION  FROM ' ||l_schema_name||'.'||l_version_table_name ||' order by APPLIED_DATE desc) a
						limit 1';
						EXECUTE l_sql_txt INTO l_out_version;
					ELSE
						l_out_version := 'No data found in  version table column APPLIED_DATE '||IN_SCHEMA_NAME||'.'||IN_VERSION_TABLE_NAME;
					END IF;
				ELSE
					SELECT COUNT(1) INTO l_ver_col_exist
					  FROM information_schema.columns
					 WHERE TABLE_NAME = l_version_table_name
					   AND table_schema = l_schema_name
					   AND COLUMN_NAME='release_date';

					IF l_ver_col_exist = 1 THEN
						--Check if version table contains any data
						l_sql_txt:='SELECT COUNT(1) from '||'(SELECT VERSION  FROM ' ||l_schema_name||'.'||l_version_table_name ||' order by RELEASE_DATE desc) a
										 limit 1';
						EXECUTE l_sql_txt INTO l_ver_exist;
							IF l_ver_exist=1 THEN
							l_sql_txt:='SELECT VERSION from '||'(SELECT VERSION  FROM ' ||l_schema_name||'.'||l_version_table_name ||' order by RELEASE_DATE desc) a
										 limit 1';
							EXECUTE l_sql_txt INTO l_out_version;
						ELSE
						l_out_version:='No data found in version table column RELEASE_DATE '||l_schema_name||'.'||l_version_table_name;
						END IF;
					ELSE
						l_out_version := 'Version could not be found for table '||l_version_table_name||' , as it does not support standard SBS verion structure ';
					END IF;
				END IF;
			END IF;
		ELSE
			l_out_version:='Version table '||l_version_table_name||' does not exist in the schema '|| l_schema_name;
		END IF;
	ELSE
		IF l_schema_name IS NULL THEN
        	l_error_msg := 'Schema name not provided';
        	l_out_version := l_error_msg;
		END IF;
		IF l_version_table_name IS NULL THEN
        	l_error_msg := 'Version table name not provided';
        	IF LENGTH(l_out_version) > 0 THEN
          		l_out_version := l_out_version|| ' and ' ||l_error_msg;
        	ELSE
          		l_out_version := l_error_msg;
        	END IF;
		END IF;
	END IF;
    RETURN (l_out_version);
EXCEPTION
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in function :- get_version(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;