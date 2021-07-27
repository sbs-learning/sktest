CREATE OR REPLACE FUNCTION p_util_get_partition_value
(
	in_schema_nm 		CHARACTER VARYING,
	in_table_name 		CHARACTER VARYING,
	in_partition_name 	CHARACTER VARYING
)
RETURNS CHARACTER VARYING
LANGUAGE 'plpgsql'
	SECURITY DEFINER
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
   /* --------------------------------------------------------------------------
	-- Purpose : Purpose of this Procedure is to find partition high value of a given partition in a Table in a particular Schema. This support only range partition name.
    -- Parameter Details
		-- in_schema_nm : Name of the Schema where the Table is present and is to be analyzed.
		-- in_table_name  : Name of the Table which is to be analyzed.
		-- in_partition_name : Partition name for which high value needs to be find out.
      -----------------------------------------------------------------------------------------------------------------------------
   --
   --------------------------------------------------------------------------------
   -- Name: p_util_get_partition_value
   --------------------------------------------------------------------------------
   --
   -- Description:  p_util_get_partition_value
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Sakshi Jain	 CSPUBCC-4306	03/05/2021	p_util_get_partition_value() initial draft*/
   
    l_format_call_stack TEXT;
	l_table_exists  	SMALLINT;
	l_schema_exist 		SMALLINT;
    v_partition_value   VARCHAR(200);
	l_schema_nm 		VARCHAR(200);
	l_table_name		VARCHAR(200);
	l_partition_name	VARCHAR(200);
BEGIN

	l_schema_nm			:= LOWER(TRIM(in_schema_nm));
	l_table_name		:= LOWER(TRIM(in_table_name));
	l_partition_name	:= LOWER(TRIM(in_partition_name));

	IF l_schema_nm = '' or l_table_name = '' or l_partition_name = '' THEN
		RAISE EXCEPTION USING ERRCODE = 50003;
	END IF;
	
	--validate schema and table name
    IF l_schema_nm IS NOT NULL AND l_table_name IS NOT NULL AND l_partition_name IS NOT NULL THEN
		SELECT COUNT(1) INTO l_schema_exist 
		  FROM information_Schema.schemata 
		 WHERE SCHEMA_NAME = l_schema_nm; 
		
		IF l_schema_exist = 1 THEN		
			SELECT COUNT(1) INTO l_table_exists
			  FROM information_Schema.tables
			 WHERE table_schema = l_schema_nm 
			   AND table_name = l_table_name;
			   
				IF l_table_exists = 0 THEN
					RAISE INFO 'table not exist in schema';
					RAISE EXCEPTION USING errcode = 50001;
				END IF;
		ELSE
			RAISE EXCEPTION USING errcode = 50002;
		END IF;
		
		SELECT pub_admin.get_partition_value(l_schema_nm, l_table_name, l_partition_name) 
		  INTO v_partition_value;
	ELSE
			--schema name or table name or partition nameis NULL
		RAISE EXCEPTION USING ERRCODE = 50003;
	END IF;
	
	RETURN v_partition_value;
   
EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in function :- p_util_get_partition_value(), table ' , in_table_name , ' not exists in schema ' , in_schema_nm , CHR(10) , ' SQLERRM :- ' , SQLERRM),
				in_show_stack		=> TRUE
				);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- p_util_get_partition_value(), schema name ', in_schema_nm, ' does not exists ', CHR(10), 'SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN  
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in function :- p_util_get_partition_value(), input parameter passed for in_schema_nm or in_table_name or in_partition_name is NULL or empty' , CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
				);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in function :- p_util_get_partition_value(), ', CHR(10), 'SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;