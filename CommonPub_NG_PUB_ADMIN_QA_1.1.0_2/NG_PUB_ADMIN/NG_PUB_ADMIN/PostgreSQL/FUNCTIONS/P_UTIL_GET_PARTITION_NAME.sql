CREATE OR REPLACE FUNCTION p_util_get_partition_name
(
	in_schema_nm		CHARACTER VARYING,
	in_table_name		CHARACTER VARYING,
	in_part_value		CHARACTER VARYING,
	in_subpart_value	CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING
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
		--in_schema_nm : Name of the Schema where the Table is present and is to be analyzed.
		--in_table_name  : Name of the Table which is to be analyzed.
		--in_part_value  : Partition value against whcih Partition name needs to be find out.
		--in_subpart_value : Sub Partition value against whcih Partition name needs to be find out. Not in use as of now
      -----------------------------------------------------------------------------------------------------------------------------
   --
   --------------------------------------------------------------------------------
   -- Name: p_util_get_partition_name
   --------------------------------------------------------------------------------
   --
   -- Description:  p_util_get_partition_name
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Kalyan Kumar	 CSPUBCC-4306	03/05/2021	p_util_get_partition_name() initial draft
   */

	
	l_schema_nm						CHARACTER VARYING(100);
	l_table_name					CHARACTER VARYING(100);
	l_part_value					CHARACTER VARYING(100);
	l_subpart_value					CHARACTER VARYING(100);
	l_format_call_stack				TEXT;
	out_partition_name				VARCHAR(100) := NULL;
	l_table_exists					VARCHAR(1);

BEGIN

	l_schema_nm		:= 	LOWER(TRIM(in_schema_nm));
	l_table_name	:= 	LOWER(TRIM(in_table_name));
	l_part_value	:= 	TRIM(in_part_value);
	l_subpart_value	:= 	TRIM(in_subpart_value);

	IF l_schema_nm IS NULL or l_schema_nm = '' THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;
	IF l_table_name IS NULL or l_table_name = '' THEN
		RAISE EXCEPTION USING ERRCODE = 50002;
	END IF;
	IF l_part_value IS NULL or l_part_value = '' THEN
		RAISE EXCEPTION USING ERRCODE = 50003;
	END IF;

	IF l_schema_nm IS NOT NULL AND l_table_name IS NOT NULL THEN
        BEGIN
            SELECT 'Y'
              INTO STRICT l_table_exists
              FROM information_schema.tables
             WHERE table_schema = l_schema_nm
			   AND TRIM(table_name,'"') = l_table_name;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
				RAISE EXCEPTION USING ERRCODE = 50004;
        END;
    END IF;
	
	SELECT pub_admin.get_partition_name(l_schema_nm, l_table_name, l_part_value, l_subpart_value)
	INTO out_partition_name;

	RETURN out_partition_name;

EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- p_util_get_partition_name(), Schema Name cannot be null or empty. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- p_util_get_partition_name(), Table Name cannot be null or empty. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- p_util_get_partition_name(), Partition value cannot be null or empty. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- p_util_get_partition_name(), Schema name or Table name does not exists.  Schema name passed - ',in_schema_nm, ', Table name passed - ',in_table_name, CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- p_util_get_partition_name(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
