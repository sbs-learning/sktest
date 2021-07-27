CREATE OR REPLACE PROCEDURE sbs_util.drop_partition
(
	p_owner				CHARACTER VARYING,
	p_table_name		CHARACTER VARYING,
	p_partition_name	CHARACTER VARYING
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   /* --------------------------------------------------------------------------
	-- Purpose : Purpose of this Procedure is to drop partition in a Table in a particular Schema.
    -- Parameter Details
	--p_owner : Name of the Schema.
	--p_table_name  : Name of the Table.
	--p_partition_name : Partition name.
   --------------------------------------------------------------------------------
   -- Name: drop_partition
   --------------------------------------------------------------------------------
   --
   -- Description:  drop_partition
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Sakshi Jain	 CSPUBCC-4306	03/05/2021	drop_partition() initial draft
   -- 	2	Akshay	     CSPUBCC-4383	04/14/2021	Added variable l_owner, l_table_name, l_partition_name
   --                                               and removed upper function from query
   --   3   Akshay       CSPUBCC-4533   06/04/2021  Handle empty string for input parameters.*/

    l_format_call_stack 	TEXT;
	l_table_exists			SMALLINT;
	l_schema_exist			SMALLINT;
	l_partition_exist		SMALLINT;
	l_query					VARCHAR;
	l_owner					VARCHAR(100);
	l_table_name			VARCHAR(100);
	l_partition_name		VARCHAR(100);
	
BEGIN
	l_owner				:= LOWER(TRIM(p_owner));
	l_table_name		:= LOWER(TRIM(p_table_name));
	l_partition_name	:= TRIM(p_partition_name);
	
	IF l_owner = '' OR l_table_name = '' THEN
		RAISE EXCEPTION USING ERRCODE = 50006;
	END IF;
	
	--validate schema and table name
    IF l_owner IS NOT NULL AND l_table_name IS NOT NULL THEN
     	SELECT COUNT(1) INTO l_schema_exist FROM information_Schema.schemata WHERE schema_name = l_owner;
			IF l_schema_exist = 1 THEN
				SELECT COUNT(1) INTO l_table_exists
				  FROM information_Schema.tables
				 WHERE table_schema = l_owner 
				   AND table_name = l_table_name;
				   
					IF l_table_exists = 0 THEN
						RAISE INFO 'table not exist in schema';
						RAISE EXCEPTION USING ERRCODE = 50001;
					END IF;
			ELSE
				RAISE EXCEPTION USING ERRCODE = 50004;
			END IF;

			IF l_partition_name IS NULL OR l_partition_name = '' THEN
				RAISE EXCEPTION USING ERRCODE = 50002;
			END IF;
			
			IF(l_partition_name IS NOT NULL) THEN
					  --validate if partition name exist in table
				 SELECT COUNT(1) INTO l_partition_exist 
				  FROM (SELECT pt.relname AS partition_name, 
							   pg_get_expr(pt.relpartbound, pt.oid, TRUE) AS partition_expression
						  FROM pg_class base_tb
						  JOIN pg_inherits i ON i.inhparent = base_tb.oid
						  JOIN pg_class pt ON pt.oid = i.inhrelid
						  JOIN pg_namespace nsp ON nsp.oid = base_tb.relnamespace
						 WHERE base_tb.relname = l_table_name::name
						   AND nsp.nspname = l_owner::name) a 
				 WHERE a.partition_name = l_partition_name::name;

				IF(l_partition_exist = 1) THEN
					l_query := 'DROP TABLE ' || l_owner || '.' ||l_partition_name ;
					EXECUTE l_query;
					RAISE INFO '%',l_query;										
				ELSE
					--partition name not exist in table
					RAISE EXCEPTION USING ERRCODE = 50003;
				END IF;
			END IF;
	ELSE
		--schema name or table nameis NULL
		RAISE EXCEPTION USING ERRCODE = 50005;
	END IF;
EXCEPTION
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(),schema name not exist ', p_owner, CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50001' THEN
	    GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(),table ', p_table_name, ' not exists in schema ', p_owner, CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
	    GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(),input parameter p_partition_name is NULL or Empty. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
	    GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(), partition name ', p_partition_name, '  not exist in table ', p_table_name, CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50005' THEN
	    GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(),input parameter passed for p_owner or p_table_name is NULL. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50006' THEN
	    GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
   		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(),input parameter p_owner or p_table_name is Empty. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(). ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
