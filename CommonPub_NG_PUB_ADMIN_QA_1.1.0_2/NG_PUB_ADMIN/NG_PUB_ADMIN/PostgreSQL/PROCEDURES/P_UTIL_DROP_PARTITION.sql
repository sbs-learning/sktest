CREATE OR REPLACE PROCEDURE p_util_drop_partition
(
	in_schema_nm		CHARACTER VARYING,
	in_table_name		CHARACTER VARYING,
	in_partition_name	CHARACTER VARYING,
	in_partition_value	CHARACTER VARYING
)
LANGUAGE 'plpgsql'
SECURITY DEFINER
AS $BODY$
DECLARE
   /* --------------------------------------------------------------------------
	-- Purpose : This is the wrapper procedure, Purpose of this Procedure is to call drop Parttion procedure of PubAdmin.
    -- Parameter Details
	--  in_schema_nm : Name of the Schema.
	--  in_table_name  : Name of the Table.
	--  in_partition_name : Partition name.
	--  in_partition_value : Partition value.
   --------------------------------------------------------------------------------
   -- Name: p_util_drop_partition
   --------------------------------------------------------------------------------
   --
   -- Description:  p_util_drop_partition
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Kalyan Kumar	 CSPUBCC-4479	04/30/2021	p_util_drop_partition() initial draft*/

	l_schema_nm 		CHARACTER VARYING(100);
	l_table_name 		CHARACTER VARYING(100);
	l_partition_name 	CHARACTER VARYING(100);
	l_partition_value 	CHARACTER VARYING(100);
	l_format_call_stack	TEXT;
BEGIN

	l_schema_nm			:= LOWER(TRIM(in_schema_nm));
	l_table_name		:= LOWER(TRIM(in_table_name));
	l_partition_name	:= LOWER(TRIM(in_partition_name));
	l_partition_value	:= LOWER(TRIM(in_partition_value));
	

	CALL pub_admin.drop_partition
		(
			in_schema_nm		=> l_schema_nm,
			in_table_name		=> l_table_name,
			in_partition_name	=> l_partition_name,
			in_partition_value	=> l_partition_value
		);

EXCEPTION
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_drop_partition(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
