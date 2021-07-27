CREATE OR REPLACE PROCEDURE sbs_util.gather_table_stats
(
	in_schema_nm 					CHARACTER VARYING,
	in_table_nm 					CHARACTER VARYING,
	in_gather_stale_only_flg 		BOOLEAN DEFAULT FALSE,
	INOUT out_gathered_stats_flg 	CHARACTER VARYING DEFAULT 'N'::CHARACTER VARYING
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   /* --------------------------------------------------------------------------
	-- Purpose : Purpose of this Procedure is to Analyze a given Table in a particular Schema.
    -- Parameter Details
	-- in_schema_nm : Name of the Schema where the Table is present and is to be analyzed.
	-- in_table_nm  : Name of the Table which is to be analyzed.
   --------------------------------------------------------------------------------
   -- Name: gather_table_stats
   --------------------------------------------------------------------------------
   --
   -- Description:  gather_table_stats
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Sakshi Jain	 CSPUBCC-4304	03/05/2021	gather_table_stats() initial draft
   -- 	2	Akshay	     CSPUBCC-4383	04/14/2021	Added variable l_schema_nm, l_table_nm, l_gathered_stats_flg
   --                                               and removed upper function from query
   --   2   Akshay       CSPUBCC-4533   06/04/2021  Handle empty string for input parameters.
   */

    l_format_call_stack		TEXT;
	l_table_exists			SMALLINT;
	l_schema_exist			SMALLINT;
	l_schema_nm				VARCHAR(100);
	l_table_nm				VARCHAR(100);
	l_gathered_stats_flg	VARCHAR(1);
BEGIN
    l_schema_nm		:= LOWER(TRIM(in_schema_nm));
	l_table_nm 		:= LOWER(TRIM(in_table_nm));

    IF l_schema_nm = '' OR l_table_nm = '' THEN
	   RAISE EXCEPTION USING ERRCODE = 50005;
	END IF;

    IF l_schema_nm IS NOT NULL and l_table_nm IS NOT NULL THEN
     	SELECT COUNT(1) INTO l_schema_exist FROM information_Schema.schemata WHERE schema_name = l_schema_nm;
			IF l_schema_exist = 1 THEN
				SELECT COUNT(1) INTO l_table_exists
				  FROM information_Schema.tables
				 WHERE table_schema = l_schema_nm AND table_name = l_table_nm;
					IF l_table_exists = 0 THEN
						RAISE INFO 'Table not exist in schema';
						RAISE EXCEPTION USING ERRCODE = 50001;
					END IF;
			ELSE
				RAISE EXCEPTION USING ERRCODE = 50004;
			END IF;

        IF l_table_exists = 1  THEN
			if(in_gather_stale_only_flg = FALSE) THEN
				EXECUTE 'analyze ' || l_schema_nm||'.'||l_table_nm;
				RAISE INFO 'analyzed %.%' , l_schema_nm, l_table_nm;
				out_gathered_stats_flg := 'Y';
			ELSE
			--This return 'Y' only when modified_rcrds count is greater than analyze_threshold
				SELECT sbs_util.are_objects_stats_stale(l_schema_nm,l_table_nm) INTO l_gathered_stats_flg;
				IF(l_gathered_stats_flg = 'Y') THEN
					EXECUTE 'analyze ' || l_schema_nm||'.'||l_table_nm;
					RAISE INFO 'analyzed %.%' , l_schema_nm, l_table_nm;
					out_gathered_stats_flg := 'Y';
				ELSE   ----l_gathered_stats_flg := 'N';
					RAISE INFO 'No stale objects found NOT analyzed %.%' , l_schema_nm, l_table_nm;
					out_gathered_stats_flg := 'N';
				END IF;
			END IF;
        END IF;
    ELSIF l_schema_nm IS NULL THEN
        RAISE INFO 'schema name is null';
		RAISE EXCEPTION USING ERRCODE = 50002;
    ELSIF l_table_nm IS NULL THEN
        RAISE INFO 'table name is null';
		RAISE EXCEPTION USING ERRCODE = 50003;
    END IF;
	RAISE info 'inside gather_table_stats() out : % ', out_gathered_stats_flg;
EXCEPTION
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- gather_table_stats(),schema name not exist ' , l_schema_nm , CHR(10) , ' SQLERRM:- ', SQLERRM), 
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50001' THEN
	    GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- gather_table_stats(),table ' , l_table_nm , ' not exists in schema ' , l_schema_nm , CHR(10) , ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;	
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- gather_table_stats(),input parameter passed for in_schema_nm for schema name is NULL. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;	
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- gather_table_stats(),input parameter passed for in_table_nm for table name is NULL. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50005' THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;	
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- gather_table_stats(),input parameter in_schema_nm or in_table_nm is Empty. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- gather_table_stats(). ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
