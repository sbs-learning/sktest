CREATE OR REPLACE PROCEDURE sbs_util.refresh_mv
(
	in_schema_nm		CHARACTER VARYING,
	in_mview_name_lst	CHARACTER VARYING,
	with_data_flg		CHARACTER VARYING DEFAULT 'Y'::CHARACTER VARYING
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   -----------------------------------------------------------------------------
   -- Name :REFRESH_MV                                             
   -----------------------------------------------------------------------------
   -- Description : To refresh mv of a particular schema.
   -- Parameters: IN_SCHEMA_NM, IN_MVIEW_NAME_LST, WITH_DATA_FLG
   ------------------------------------------------------------------------------
   -- RefNo  Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- -------------------------------------------
   -- 1      Akshay        CSPUBCC-4383   4/15/2021  Added variable l_with_data_flg, l_schema_nm, l_mview_name_list,
   --                                                removed upper function from query and added exception block.                                                                                                  
   ------------------------------------------------------------------------------
   
	mv_array             VARCHAR ARRAY;
	l_mv_name            VARCHAR(64);
	l_mv_exists          smallint;
	v_sql                VARCHAR(4000);
	l_user               VARCHAR(4000);
	l_with_data_flg      VARCHAR(1);
	l_schema_nm          VARCHAR(200);
	l_mview_name_list    VARCHAR(4000);
	l_format_call_stack  TEXT;
BEGIN
    l_with_data_flg   := TRIM(UPPER(WITH_DATA_FLG));
	l_schema_nm       := TRIM(LOWER(IN_SCHEMA_NM));
	l_mview_name_list := TRIM(LOWER(IN_MVIEW_NAME_LST));

	IF l_with_data_flg NOT IN ('Y','N') THEN
		RAISE INFO 'WITH_DATA_FLG input parameter have invalid value, it should be Y or N';
		RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;
	
	IF ((l_with_data_flg is NULL) or (l_with_data_flg = '')) THEN
		RAISE INFO 'WITH_DATA_FLG input parameter have invalid value, it should be Y or N';
		RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;
	
	IF l_schema_nm IS NULL or l_schema_nm = ''THEN
		RAISE INFO 'Schema name is NULL or empty';
		RAISE EXCEPTION USING ERRCODE = 50002;
	ELSIF l_mview_name_list IS NULL or l_mview_name_list = '' THEN
		RAISE INFO 'Mv name is NULL or empty';
		RAISE EXCEPTION USING ERRCODE = 50003;
	ELSIF l_schema_nm IS NOT NULL AND l_mview_name_list IS NOT NULL THEN
		l_mv_name := l_mview_name_list;
	 
		SELECT string_to_array(l_mview_name_list, ',') INTO strict mv_array;

	    FOR I IN array_lower(mv_array, 1)..array_upper(mv_array, 1)
		LOOP
			l_mv_name = mv_array[I];

			BEGIN			
				SELECT LOWER(CURRENT_USER) INTO l_user; 
				l_mv_exists = 0;
				
				SELECT COUNT(1)
				  INTO l_mv_exists 
				  FROM pg_matviews
			     WHERE schemaname = l_schema_nm 
				   AND matviewname = l_mv_name;						 

				IF(l_mv_exists = 1) THEN
					RAISE INFO '%',l_mv_name;
					IF(l_with_data_flg = 'Y')THEN
						v_Sql := 'REFRESH MATERIALIZED VIEW ' || l_schema_nm ||'.'||l_mv_name || ' WITH DATA';
						EXECUTE v_sql;
						RAISE INFO '%',v_Sql;				 
					ELSE
						v_Sql := 'REFRESH MATERIALIZED VIEW ' || l_schema_nm||'.'||l_mv_name || ' WITH NO DATA';
						EXECUTE v_sql;
						RAISE INFO '%',v_Sql;
					END IF;
				ELSE 
					RAISE INFO 'Materialized View: % do not exist in schema: %', l_mv_name,l_schema_nm;
					RAISE EXCEPTION USING ERRCODE = 50005;
				END IF;
			END;
		END LOOP;
	END IF;
EXCEPTION  
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;	 
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in procedure :- REFRESH_MV(), parameter WITH_DATA_FLG have invalid value, it should be Y or N . ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- REFRESH_MV(), parameter IN_SCHEMA_NM not accept null or '' value. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- REFRESH_MV(), parameter IN_MVIEW_NAME_LST not accept null or '' value. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50005' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in procedure :- REFRESH_MV(), materialized view ', l_mv_name, ' do not exist in schema ', l_schema_nm, CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
	    GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in Procedure :- REFRESH_MV(),', CHR(10), ' SQLERRM:-', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;