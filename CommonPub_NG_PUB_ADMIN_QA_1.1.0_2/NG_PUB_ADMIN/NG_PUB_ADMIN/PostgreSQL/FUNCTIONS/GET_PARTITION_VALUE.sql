CREATE OR REPLACE FUNCTION pub_admin.get_partition_value
(
	in_schema_nm		CHARACTER VARYING,
	in_table_name 		CHARACTER VARYING,
	in_partition_name 	CHARACTER VARYING
)
    RETURNS CHARACTER VARYING
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
   /* --------------------------------------------------------------------------
	-- Purpose : Purpose of this Procedure is to find partition high value of a given partition in a Table in a particular Schema. This support only range partition name.
    -- Parameter Details
		--in_schema_nm : Name of the Schema where the Table is present and is to be analyzed.
		--in_table_name  : Name of the Table which is to be analyzed.
		--in_partition_name : Partition name for which high value needs to be find out.
      -----------------------------------------------------------------------------------------------------------------------------
   --
   --------------------------------------------------------------------------------
   -- Name: get_partition_value
   --------------------------------------------------------------------------------
   --
   -- Description:  get_partition_value
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Sakshi Jain	 CSPUBCC-4306	03/05/2021	get_partition_value() initial draft
   */
   
	l_format_call_stack TEXT;
	l_table_exists		SMALLINT;
	l_schema_exist		SMALLINT;
	v_partition_val		NUMERIC;
	v_high_value		VARCHAR;
	l_partition_exist	SMALLINT;
	l_part_bound		VARCHAR;
	l_partition_type	VARCHAR(10);

	l_schema_nm			VARCHAR(100);
	l_table_name     	VARCHAR(100);
	l_partition_name 	VARCHAR(100);
	
BEGIN

	l_schema_nm			:= LOWER(TRIM(in_schema_nm));
	l_table_name		:= LOWER(TRIM(in_table_name));
	l_partition_name	:= LOWER(TRIM(in_partition_name));

	--validate schema and table name
    IF l_schema_nm IS NOT NULL AND l_table_name IS NOT NULL AND l_partition_name IS NOT NULL THEN
     	SELECT COUNT(1) INTO l_schema_exist
		  FROM information_Schema.schemata
		 WHERE schema_name = l_schema_nm;

		IF l_schema_exist = 1
		THEN
			SELECT COUNT(1) INTO l_table_exists
			  FROM information_Schema.tables
			 WHERE table_schema = l_schema_nm
			   AND table_name = l_table_name;

			IF l_table_exists = 0
			THEN
				RAISE INFO 'table not exist in schema';
				RAISE EXCEPTION USING ERRCODE = 50001;
			END IF;
		ELSE
			RAISE EXCEPTION USING ERRCODE = 50004;
		END IF;

      --validate if partition name exist in table
		SELECT COUNT(1)
		  INTO l_partition_exist 
		  FROM (
			  		SELECT pt.relname AS partition_name,
						   pg_get_expr(pt.relpartbound, pt.oid, TRUE) AS partition_expression
					  FROM pg_class base_tb
					  JOIN pg_inherits i ON i.inhparent = base_tb.oid
					  JOIN pg_class pt ON pt.oid = i.inhrelid
					  JOIN pg_namespace pgnsp ON pgnsp.oid = base_tb.relnamespace
					 WHERE base_tb.relname = l_table_name::name
					 and pgnsp.nspname = l_schema_nm::name
				) a
		WHERE a.partition_name = l_partition_name;

		IF(l_partition_exist = 1) THEN
		   --find relbound, type of the provided partition
			SELECT a.partition_expression AS part_bound,
			       a.partition_type
			  INTO l_part_bound , l_partition_type
			 FROM (SELECT base_tb.relname::name AS base_table,
			              pt.relname AS partition_name,
						  pt.relkind AS partition_type,
						  pg_get_expr(pt.relpartbound, pt.oid, TRUE) AS partition_expression
				     FROM pg_class base_tb 
					 JOIN pg_inherits i ON i.inhparent = base_tb.oid 
				     JOIN pg_class pt ON pt.oid = i.inhrelid
					 JOIN pg_namespace pgnsp ON pgnsp.oid = base_tb.relnamespace
				    WHERE base_tb.relname = l_table_name::name
					and pgnsp.nspname = l_schema_nm::name
					) a
			 WHERE a.base_table = l_table_name::name 
			   AND a.partition_name = l_partition_name;
  	
			IF(l_partition_type = 'r') THEN
				SELECT SUBSTR(l_part_bound,
							 (LENGTH(SUBSTR(l_part_bound,0,POSITION('TO' IN l_part_bound))) + 4 ),
							 (LENGTH(l_part_bound)))
					INTO v_high_value;
				RAISE INFO 'v_high_value: %' , v_high_value ;
			ELSE
				--partition is not range type
				RAISE EXCEPTION USING errcode = 50002;
			END IF;
		ELSE
			--partition name not exist in table
			RAISE EXCEPTION USING errcode = 50003;
		END IF;
	ELSE
		--schema name or table name or partition nameis NULL
		RAISE EXCEPTION USING errcode = 50005;
	END IF;
	
	RETURN RTRIM(LTRIM(v_high_value,'('''),''')');
   
EXCEPTION
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_value(), schema name ' , in_schema_nm , ' does not exists ', chr(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_value(), table ', in_table_name, ' not exists in schema ' , in_schema_nm, CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_value(), input partition is not range type ' , in_partition_name , CHR(10) , ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_value(), partition name ' , in_partition_name , '  not exist in table ' , in_table_name , CHR(10) , ' SQLERRM :-  ' , SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50005' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_value(), input parameter passed for in_schema_nm or in_table_name or in_partition_name is NULL. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_value(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
