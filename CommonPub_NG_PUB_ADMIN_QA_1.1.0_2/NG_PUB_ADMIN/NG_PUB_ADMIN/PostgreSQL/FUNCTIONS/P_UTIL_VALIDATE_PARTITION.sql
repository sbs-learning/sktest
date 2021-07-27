CREATE OR REPLACE FUNCTION p_util_validate_partition
(
	in_fully_qualified_tbl_nm	CHARACTER VARYING,
	in_part_val 				CHARACTER VARYING
)
RETURNS CHARACTER VARYING
LANGUAGE 'plpgsql'
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
DECLARE
    -------------------------------------------------------------------------------------
    -- Name : P_UTIL_VALIDATE_PARTITION
    -------------------------------------------------------------------------------------
    -- Description : This function will check whether the table is partition or not.
	--               It will check whether the partition table name contains any records or not.
	--               It will also check whether the partition value fit to any partition or not.
    -- 
    -- Parameters : in_fully_qualified_tbl_nm, in_part_val
	----------------------------------------------------------------------------------
    -- RefNo Name            JIRA NO 		   Date           Description of change
    -- ----- ---------------- -------- ---------------------------------------------
    -- 	1	Akshay  	    CSPUBCC-4356	 5/3/2021        p_util_validate_partition() initial draft
    -------------------------------------------------------------------------------------

	l_fully_qualified_tbl_nm  VARCHAR(100);
	l_part_val                VARCHAR(100);
	l_schema_name             VARCHAR(100);
    l_table_name              VARCHAR(100);
    l_data_exits              SMALLINT;
    l_return                  VARCHAR(4000);
	l_col_exists              VARCHAR(1);
	l_partition_table_name    VARCHAR(100);
	l_sql_txt                 VARCHAR(4000);
	l_format_call_stack       TEXT;

BEGIN
		l_fully_qualified_tbl_nm:= LOWER(TRIM(in_fully_qualified_tbl_nm));
		l_part_val:= TRIM(in_part_val);
	  
	  	IF (l_fully_qualified_tbl_nm IS NULL) THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
	    END IF;
		
		IF (l_part_val IS NULL) THEN
		RAISE EXCEPTION USING ERRCODE = 50003;
	    END IF;
	  
		l_schema_name:= SUBSTR(l_fully_qualified_tbl_nm,0,POSITION('.' IN l_fully_qualified_tbl_nm));
		l_table_name := SUBSTR(l_fully_qualified_tbl_nm,POSITION('.' IN l_fully_qualified_tbl_nm)+1,LENGTH(l_fully_qualified_tbl_nm));
	  
	  	IF ((l_schema_name IS NULL OR l_schema_name = '') OR (l_table_name IS NULL OR l_table_name = '')) THEN
		RAISE EXCEPTION USING ERRCODE = 50004;
	    END IF;
		
		SELECT pub_admin.get_partition_name
	  	             (
	  	             	in_schema_nm       => l_schema_name,
                        in_table_name      => l_table_name,
	  	             	in_part_value 	   => l_part_val,
	  	             	in_subpart_value   => null
	  	              ) INTO STRICT l_partition_table_name;
				
				IF l_partition_table_name IS NOT NULL 
			      THEN
				      l_sql_txt := 'SELECT count(1) FROM ' || l_schema_name || '.' || l_partition_table_name;
				END IF;
				
				RAISE INFO 'l_sql_txt - %',l_sql_txt;
				
				EXECUTE l_sql_txt INTO l_data_exits;
				
				IF l_data_exits >= 1 
				   THEN
				       l_return := 'VALID_PARTITION_WITH_DATA';
				ELSIF l_data_exits = 0
				   THEN
					   l_return := 'VALID_PARTITION_WITHOUT_DATA';
				END IF;
					
    RETURN l_return;

 EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in function :- p_util_validate_partition(), input parameter in_fully_qualified_tbl_nm parameter cannot have null or empty value. ', CHR(10), ' SQLERRM :-  ', SQLERRM),
				in_show_stack 		=> TRUE
			);			
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in function :- p_util_validate_partition(), input parameter in_part_val parameter cannot have null or empty value. ', CHR(10), ' SQLERRM :-  ', SQLERRM),
				in_show_stack 		=> TRUE
			);
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in function :- p_util_validate_partition(), input parameter fully qualified table name is not passed correctly as schema_name.table_name. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack 		=> TRUE
			);
    WHEN OTHERS THEN
	  GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- p_util_validate_partition(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;