CREATE OR REPLACE PROCEDURE p_util_add_range_partition(
	in_schema_name      CHARACTER VARYING,
	in_table_name       CHARACTER VARYING,
	in_partition_name   CHARACTER VARYING,
	in_partition_value  CHARACTER VARYING)
	
LANGUAGE 'plpgsql'
SECURITY DEFINER
AS $BODY$
DECLARE

  ------------------------------------------------------------------------------------------------
   --Name : P_UTIL_ADD_RANGE_PARTITION
   ------------------------------------------------------------------------------------------------
   -- Description : This procedure is a wrapper procedure which is used to call procedure add_range_partition
   --               which will create partition on existing Range Partitioned Table.
   --
   -- Parameters : in_schema_name, in_table_name, in_partition_name, in_partition_value
   --
   -----------------------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		   Date           Description of change
   -- ----- ---------------- -------- ------------------------------------------------------------
   -- 	1	Akshay  	    CSPUBCC-4355	  6/10/2021       p_util_add_range_partition() initial draft
   -----------------------------------------------------------------------------------------------

	l_schema_name		 VARCHAR(100);
	l_table_name		 VARCHAR(100);
	l_partition_name	 VARCHAR(100);
	l_partition_value	 VARCHAR(100);
	l_format_call_stack	 TEXT;

BEGIN
	l_schema_name:= LOWER(TRIM(in_schema_name));
	l_table_name:= LOWER(TRIM(in_table_name));
	l_partition_name:= LOWER(TRIM(in_partition_name));
	l_partition_value:= TRIM(in_partition_value);

	IF l_schema_name IS NULL or l_schema_name = '' THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;
	IF l_table_name IS NULL or l_table_name = '' THEN
		RAISE EXCEPTION USING ERRCODE = 50002;
	END IF;
	IF l_partition_name IS NULL or l_partition_name = '' THEN
		RAISE EXCEPTION USING ERRCODE = 50003;
	END IF;
	IF l_partition_value IS NULL or l_partition_value = '' THEN
		RAISE EXCEPTION USING ERRCODE = 50004;
	END IF;

	CALL pub_admin.add_range_partition(
	     in_schema_name      => l_schema_name,
         in_table_name       => l_table_name,
         in_partition_name   => l_partition_name,
		 in_partition_value  => l_partition_value);

EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in Procedure :- p_util_add_range_partition(), Schema Name cannot be null or empty. ', CHR(10), 'SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in Procedure :- p_util_add_range_partition(), Table Name cannot be null or empty. ', CHR(10), 'SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in Procedure :- p_util_add_range_partition(), Partition name cannot be null or empty. ', CHR(10), 'SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in Procedure :- p_util_add_range_partition(), Partition value cannot be null or empty. ', CHR(10), 'SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in Procedure :- p_util_add_range_partition(), ', CHR(10), 'SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;