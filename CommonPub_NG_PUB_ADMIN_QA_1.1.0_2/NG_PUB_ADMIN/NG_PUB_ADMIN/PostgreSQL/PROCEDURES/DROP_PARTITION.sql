CREATE OR REPLACE PROCEDURE pub_admin.drop_partition
(
	in_schema_nm		CHARACTER VARYING,
	in_table_name		CHARACTER VARYING,
	in_partition_name	CHARACTER VARYING,
	in_partition_value	CHARACTER VARYING
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   /* --------------------------------------------------------------------------
	-- Purpose : Purpose of this Procedure is to drop partition on the basis of name or high value in a Table in a particular Schema.
    -- Parameter Details
	--  in_schema_nm : Name of the Schema.
	--  in_table_name  : Name of the Table.
	--  in_partition_name : Partition name.
	--  in_partition_value : Partition value.
   --------------------------------------------------------------------------------
   -- Name: drop_partition
   --------------------------------------------------------------------------------
   --
   -- Description:  drop_partition
   -- Purpose - This will drop the Partion with for which it is called.
   --   		This can be called by provind Partion name or Partition value.
   --			Currently we are supporting below Partitions 
				RANGE Partition for numeric, integer, date datatypes.
				LIST Partition for char and varchar Datatypes.
			Also the date datatype should be in the format - YYYY-MM-DD only.
			If partition created on functional columns it will not drop partition, as functional columns are not supported.
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Sakshi Jain	 CSPUBCC-4306	03/05/2021	drop_partition() initial draft*/

    l_format_call_stack				TEXT;
	l_table_exists					SMALLINT;
	l_schema_exist					SMALLINT;
	l_partition_exist				VARCHAR(1);
	l_cons_exist					SMALLINT;
	l_child_table_count				SMALLINT;
	l_child_table_fk_count			SMALLINT;
	l_query							VARCHAR(4000);
	l_get_partition_name			VARCHAR(100);
	l_schema_nm						CHARACTER VARYING(100);
	l_table_name					CHARACTER VARYING(100);
	l_partition_name				CHARACTER VARYING(100);
	l_partition_value				CHARACTER VARYING(100);
	l_audit_user					CHARACTER VARYING(100);
	l_constraint_table				CHARACTER VARYING(100);
	l_constraint_column				CHARACTER VARYING(100);
	l_partition_expression  		CHARACTER VARYING(500);
	l_part_high_value_in_number		VARCHAR (1000);
	l_part_high_value_in_number1	VARCHAR (1000);
	l_part_high_value_in_from		VARCHAR (1000);
	l_part_high_value_in_to			VARCHAR (1000);
  	v_partitioning_type				VARCHAR (4000);
  	v_number_of_partition_columns	VARCHAR (4000);
	v_position_of_partition_columns	VARCHAR (4000);
	l_part_high_value_in_varchar	VARCHAR (1000);
	max_array_length				INTEGER;
	l_original_partition_value		VARCHAR (1000);
	v_rec							RECORD;
	l_partition_column_name			VARCHAR (100);
	l_partition_column_data_type	VARCHAR (1000);
	co_part_data_type_number		CONSTANT VARCHAR(100) :='NUMERIC';
	co_part_data_type_date			CONSTANT VARCHAR(100) :='DATE';
	co_part_data_type_integer		CONSTANT VARCHAR(100) :='INTEGER';
	co_part_data_type_varchar		CONSTANT VARCHAR(100) :='CHARACTER VARYING';
	co_part_data_type_char			CONSTANT VARCHAR(4) := 'CHAR';
BEGIN

	l_schema_nm			:= LOWER(TRIM(in_schema_nm));
	l_table_name		:= LOWER(TRIM(in_table_name));
	l_partition_name	:= LOWER(TRIM(in_partition_name));
	l_partition_value	:= LOWER(TRIM(in_partition_value));
	l_audit_user		:= sbs_util.get_audit_user();
	
	-- Check if Schmea Name or Table Name is passed as NULL, If yes raise an error.
	IF(l_schema_nm = '' OR l_table_name = '') THEN
		RAISE EXCEPTION USING ERRCODE = 50005;
	END IF;
	
	-- Check if partition name and parition value is passed as blank or NULL, If yes raise an error.
	IF(l_partition_name = '' OR  l_partition_name IS NULL) AND (l_partition_value IS NULL OR l_partition_value = '') THEN
		RAISE EXCEPTION USING ERRCODE = 50002;
	END IF;
	
	-- Check if partition name and parition value is passed as blank or NULL, If yes raise an error.
	IF(l_partition_value = '' AND  l_partition_name IS NOT NULL) OR (l_partition_value IS NOT NULL and l_partition_name = '') THEN
		RAISE EXCEPTION USING ERRCODE = 50000;
	END IF;
	
	--validate schema and table name, currectly passed or not. If not raise an error.
    IF l_schema_nm IS NOT NULL and l_table_name IS NOT NULL THEN
     	SELECT COUNT(1) INTO l_schema_exist 
		  FROM information_Schema.schemata 
		 WHERE schema_name = l_schema_nm;

		IF l_schema_exist = 1 THEN
			SELECT COUNT(1) INTO l_table_exists
			  FROM information_Schema.tables
		     WHERE table_schema = l_schema_nm 
			   AND table_name = l_table_name;
				   
			IF l_table_exists = 0 THEN
				RAISE INFO 'table not exist in schema';
				RAISE EXCEPTION USING ERRCODE = 50001;
			END IF;
		ELSE
			RAISE EXCEPTION USING ERRCODE = 50004;
		END IF;
		
		-- Check if partition name and parition value both are passed as blank or NULL, If yes raise an error.
		IF l_partition_name IS NULL AND l_partition_value IS NULL THEN
			RAISE EXCEPTION USING ERRCODE = 50002;
		END IF;
		
		-- Check if partition name and parition value both are passed have been passed with a value, If yes raise an error.
		-- We need either partition name or partition value.
		IF l_partition_name IS NOT NULL AND l_partition_value IS NOT NULL THEN
			RAISE EXCEPTION USING ERRCODE = 50006;
		END IF;

		-- When Partition name is null and partition value is not null, 
		-- then we need to call get_partition_name to fetch the partition name of the value provided and process ahead.
		IF in_partition_name IS NULL AND in_partition_value IS NOT NULL THEN
			l_partition_name := pub_admin.get_partition_name(l_schema_nm, l_table_name, l_partition_value);
		END IF;
		RAISE INFO 'l_partition_name - %', l_partition_name;
		
		-- Query to check if the Partition name that was provided, exists or not, if not raise en error.
		-- Also we need to fetch the minimum and maximum partition values in case of Range partition and list of values in case of List partition.
		-- l_partition_expression will hold that value.

			
		SELECT 'Y',partition_expression 
			INTO l_partition_exist, l_partition_expression
				  FROM (SELECT pt.relname AS partition_name, 
							   pg_get_expr(pt.relpartbound, pt.oid, TRUE) AS partition_expression
						  FROM pg_class base_tb
						  JOIN pg_inherits i ON i.inhparent = base_tb.oid
						  JOIN pg_class pt ON pt.oid = i.inhrelid
						  JOIN pg_namespace nsp ON nsp.oid = base_tb.relnamespace
						 WHERE base_tb.relname = l_table_name::name
						   AND nsp.nspname = l_schema_nm::name) a 
				 WHERE a.partition_name = l_partition_name::name;
			
			
			RAISE INFO 'l_partition_expression - %', l_partition_expression;
		
		-- If partition exists, then we need to check if that table has any PK and FK or not.
		-- If Constraints exists, then Handle Constraints needs to be called before drop of any partition.
		IF l_partition_exist = 'Y' THEN
			SELECT COUNT(1)
			INTO l_cons_exist
				/*pgc.conname as constraint_name,
				ccu.table_schema as table_schema,
				ccu.table_name,
				ccu.column_name,
				contype*/
			FROM pg_constraint pgc
			JOIN pg_namespace nsp ON nsp.oid = pgc.connamespace
			JOIN pg_class  cls ON pgc.conrelid = cls.oid
			LEFT JOIN information_schema.constraint_column_usage ccu
					  ON pgc.conname = ccu.constraint_name
					  AND nsp.nspname = ccu.constraint_schema
			WHERE contype IN ('p','u','f')
			  AND ccu.table_name = l_table_name 
			  AND ccu.table_schema = l_schema_nm;
			
			-- this query will fetch the partition type whether it is Range or List, and the position of the columns on which partition is being created.
			BEGIN
				SELECT CASE WHEN partstrat = 'l' then 'LIST'
							WHEN partstrat = 'r' then 'RANGE'
					   END partition_type,
					   partnatts number_of_partition_columns, partattrs position_of_partition_columns
				  INTO STRICT v_partitioning_type,
							  v_number_of_partition_columns,
							  v_position_of_partition_columns
				  FROM pg_partitioned_table pg_partitioned_table,
					   pg_class pg_class,
					   pg_namespace pg_namespace 
				 WHERE pg_class.relname = l_table_name
				   AND pg_class.relnamespace = pg_namespace.oid
				   AND pg_partitioned_table.partrelid = pg_class.oid
				   AND pg_namespace.nspname = l_schema_nm;
			EXCEPTION
				WHEN NO_DATA_FOUND THEN
					RAISE EXCEPTION USING ERRCODE = 50004;
			END;
			
			-- if more than one column position is coming, then make it a comma separated value by adding ',' in between values.
			v_position_of_partition_columns := REPLACE(v_position_of_partition_columns, ' ', ',');
			
			-- Partition Type is not Null, (the above query will only initialize Range or List to the variable, in other cases it will be assigned as NULL)
			-- then we need to find the column name and its datatype, on the basis of column position fetched above.
			-- If any functional column is being partitioned, like UPPER(Item_id), then this query will return NULL, we are not supporting functonal columns.
			-- This will raise an error.
			IF v_partitioning_type IS NOT NULL THEN
				BEGIN
					-- Query to fetch name and datatype of the partitioned columns.
					l_query :=
					'SELECT column_name, data_type
					   FROM information_schema.columns 
					  WHERE TABLE_SCHEMA = '''||l_schema_nm||'''
						AND TABLE_NAME = '''||l_table_name||'''
						AND ORDINAL_POSITION IN ('||v_position_of_partition_columns||')
						LIMIT 1';
				EXECUTE l_query INTO l_partition_column_name,l_partition_column_data_type;
				END;
			END IF;
			
			-- Check if column variables have NULL value or not. It will contain NULL only in case when partitio is created on functonal columns like UPPER(Item_id)
			IF l_partition_column_name IS NULL OR l_partition_column_data_type IS NULL THEN
				RAISE EXCEPTION USING ERRCODE = 50005;
			END IF;
			
			-- This query will fetch if there is any child table having FK constraint or not for the given table.
			-- This will fetch the count of child tables.
			WITH unnested_confkey AS (SELECT oid, unnest(confkey) as confkey FROM pg_constraint ),
							 unnested_conkey AS (SELECT oid, unnest(conkey) as conkey FROM pg_constraint)
						SELECT COUNT(1) INTO l_child_table_fk_count
								  /*c.conname                   AS constraint_name,
								  c.contype                   AS constraint_type,
								  tbl.relname                 AS constraint_table,
								  col.attname                 AS constraint_column,
								  referenced_tbl.relname      AS referenced_table,
								  referenced_field.attname    AS referenced_column,
								  pg_get_constraintdef(c.oid) AS definition*/
						  FROM pg_constraint c
						  JOIN pg_namespace nsp ON nsp.oid = c.connamespace
						  LEFT JOIN unnested_conkey con ON c.oid = con.oid
						  LEFT JOIN pg_class tbl ON tbl.oid = c.conrelid
						  LEFT JOIN pg_attribute col ON (col.attrelid = tbl.oid AND col.attnum = con.conkey)
						  LEFT JOIN pg_class referenced_tbl ON c.confrelid = referenced_tbl.oid
						  LEFT JOIN unnested_confkey conf ON c.oid = conf.oid
						  LEFT JOIN pg_attribute referenced_field ON (referenced_field.attrelid = c.confrelid AND referenced_field.attnum = conf.confkey)
						 WHERE c.contype = 'f'
						   AND conparentid = 0
						   AND referenced_tbl.relname = l_table_name
						   AND nsp.nspname = l_schema_nm;
			
			-- If PK count (l_cons_exist) is greater than 0 and FK count (l_child_table_fk_count) is also greater than 0, 
			-- we need to then fetch the column name of child table.
			-- If both the count or any one of the count is coming to be 0, it means the table is having only PK in it or no PK and no Fk at all.
			-- In that case the table will be treated as a simple table and go to ELSE condition where sbs_util.drop_partition is being called.
			IF (l_cons_exist > 0 AND l_child_table_fk_count > 0) THEN
			
				-- If PK count exists and FK count exists, then we need to find the child table name and the column name on which FK is created.
				-- If multuple tables are having FK constraint, so we need to process them in Loop.
				FOR v_rec IN 
					(
						WITH unnested_confkey AS (SELECT oid, unnest(confkey) as confkey FROM pg_constraint ),
							 unnested_conkey AS (SELECT oid, unnest(conkey) as conkey FROM pg_constraint)
						SELECT tbl.relname AS constraint_table,
							   col.attname AS constraint_column
								  /*c.conname                   AS constraint_name,
								  c.contype                   AS constraint_type,
								  tbl.relname                 AS constraint_table,
								  col.attname                 AS constraint_column,
								  referenced_tbl.relname      AS referenced_table,
								  referenced_field.attname    AS referenced_column,
								  pg_get_constraintdef(c.oid) AS definition*/
						  FROM pg_constraint c
						  JOIN pg_namespace nsp ON nsp.oid = c.connamespace
						  LEFT JOIN unnested_conkey con ON c.oid = con.oid
						  LEFT JOIN pg_class tbl ON tbl.oid = c.conrelid
						  LEFT JOIN pg_attribute col ON (col.attrelid = tbl.oid AND col.attnum = con.conkey)
						  LEFT JOIN pg_class referenced_tbl ON c.confrelid = referenced_tbl.oid
						  LEFT JOIN unnested_confkey conf ON c.oid = conf.oid
						  LEFT JOIN pg_attribute referenced_field ON (referenced_field.attrelid = c.confrelid AND referenced_field.attnum = conf.confkey)
						 WHERE c.contype = 'f'
						   AND conparentid = 0
						   AND referenced_tbl.relname = l_table_name
						   AND nsp.nspname = l_schema_nm
					)
				LOOP
					 -- When partition is Range partition and datatype of the partitioned column is NUMBER, INTEGER or DATE.
					 -- We need to find the minimum nad maximum value of that partition value.
					 -- Example - 'FOR VALUES FROM (0) TO (100)', here 0 is the minimum value and 100 is the maximum value of that partition.
					 -- From the above string 0 and 100 needs to be exracted, below code supports the extraction of values from string.
					 
					IF v_partitioning_type = 'RANGE' AND upper(l_partition_column_data_type) IN (co_part_data_type_number, co_part_data_type_date, co_part_data_type_integer)   --Partition type 'RANGE'
					THEN
						l_part_high_value_in_from := SPLIT_PART(SPLIT_PART(REPLACE(l_partition_expression, '''',''''''), 'TO', 1),'FROM',2);
						l_part_high_value_in_to := SPLIT_PART(REPLACE(l_partition_expression, '''',''''''), 'TO', 2);

						l_part_high_value_in_from := REPLACE(REPLACE(l_part_high_value_in_from, '(''', ''), ''')', '');
						l_part_high_value_in_to := REPLACE(REPLACE(l_part_high_value_in_to, '(''', ''), ''')', '');

						l_part_high_value_in_from := REPLACE(REPLACE(l_part_high_value_in_from, '(', ''), ')', '');
						l_part_high_value_in_to := REPLACE(REPLACE(l_part_high_value_in_to, '(', ''), ')', '');

						RAISE INFO 'l_part_high_value_in_from - %', l_part_high_value_in_from;
						RAISE INFO 'l_part_high_value_in_to - %', l_part_high_value_in_to;

						-- If the column of the child table having FK constraint has any value within the Minimum and maximum range, then we will exit from the Loop,
						-- stating that child table has data in it, parent partioned cannot be dropped.
						-- The count of the child table is kept in a variable l_child_table_count. 
						l_query := 'SELECT COUNT(1) FROM '|| l_schema_nm || '.' ||v_rec.constraint_table||' WHERE '||
								   v_rec.constraint_column ||' >= '|| l_part_high_value_in_from ||' and ' || v_rec.constraint_column || ' < ' ||l_part_high_value_in_to;

						RAISE INFO 'l_query - %', l_query;
						EXECUTE l_query INTO l_child_table_count;
						RAISE INFO 'l_child_table_count - %', l_child_table_count;
						IF l_child_table_count > 0 THEN 
							EXIT;
						END IF;
						
					 -- When partition is List partition and datatype of the partitioned column is Char or Varchar.
					 -- We need to find all values of that list partition.
					 -- Example - 'FOR VALUES IN ('a', 'b', 'c')', here a, b and c is the individual values for which child table needs to be checked.
					 -- From the above string a, b, c needs to be exracted, below code supports the extraction of values from string.
					 -- This is processed in loop, if child has data for any of the value it will exit the first loop.
					 -- To exit from the outer loop again a condition is checked and EXIT statement is there, else it would have processed again due to outer loop.
					ELSIF v_partitioning_type = 'LIST' AND UPPER(l_partition_column_data_type) IN (co_part_data_type_char, co_part_data_type_varchar) THEN
						l_part_high_value_in_varchar := SPLIT_PART(l_partition_expression, 'IN', 2);
						l_part_high_value_in_varchar := TRIM(RTRIM(LTRIM(l_part_high_value_in_varchar, '('),')'));
						l_part_high_value_in_varchar := TRIM(REPLACE(l_part_high_value_in_varchar, '(',''));

						-- This will fetch the number or values for which the inner loop must be executed.
						max_array_length := MAX(array_length(regexp_split_to_array(l_part_high_value_in_varchar, ','), 1));

						FOR I IN 1..max_array_length
						LOOP
							l_original_partition_value := split_part(l_part_high_value_in_varchar,',', I);

							RAISE INFO 'l_original_partition_value : %',l_original_partition_value;
							
							l_query := 'SELECT COUNT(1) FROM '|| l_schema_nm ||'.'||v_rec.constraint_table||' WHERE '||
								   v_rec.constraint_column|| ' = '||l_original_partition_value;
							
							RAISE INFO 'l_query - %', l_query;
							
							EXECUTE l_query INTO l_child_table_count;
							RAISE INFO 'l_child_table_count - %', l_child_table_count;
							IF l_child_table_count > 0 THEN 
								EXIT;
							END IF;
						END LOOP;
						
						IF l_child_table_count > 0 THEN
							EXIT;
						END IF;
					ELSE
						RAISE INFO 'We support partition RANGE/LIST TYPE';
						RAISE EXCEPTION USING ERRCODE = 50008;
					END IF;
				END LOOP;
				
				-- If child table has no data in it, then Handle constraints is being called and then drop partition is being called.
				IF l_child_table_count = 0 THEN
					RAISE info 'disable constraint';
					CALL pub_admin.handle_constraints
						(
							in_schema_nm 				=> l_schema_nm,
							in_table_nm 				=> l_table_name,
							in_action 					=> 'DISABLE',
							in_user_id 				    => l_audit_user
						);
					CALL pub_admin.p_ctrl_log_event
						(
							in_event_constant			=> 'CO_DROP_PARTITION',
							in_table_name            	=> l_table_name,
							in_event_src_cd_location 	=> 'Procedure: DROP_PARTITION',
							in_event_statement       	=> 'sbs_util.drop_partition',
							in_event_dtl             	=> 'START DROP PARTITION',
							in_user_id               	=> l_audit_user
						);
					CALL sbs_util.drop_partition
						(
							p_owner 			=> l_schema_nm, 
							p_table_name 		=> l_table_name,
							p_partition_name 	=> l_partition_name
						);
					CALL pub_admin.p_ctrl_log_event
						(
							in_event_constant        	=> 'CO_DROP_PARTITION',
							in_table_name            	=> l_table_name,
							in_event_src_cd_location 	=> 'Procedure: DROP_PARTITION',
							in_event_statement       	=> 'sbs_util.drop_partition',
							in_event_dtl             	=> 'END DROP PARTITION',
							in_user_id               	=> l_audit_user
						);
					CALL pub_admin.handle_constraints
						(
							in_schema_nm 				=> l_schema_nm,
							in_table_nm 				=> l_table_name,
							in_action 					=> 'ENABLE',
							in_user_id 				    => l_audit_user
						);
					RAISE info 'enable constraint';
				ELSE
					RAISE EXCEPTION USING ERRCODE = '50007';
				END IF;
			ELSE
			-- When PK count (l_cons_exist) = 0 or FK count (l_child_table_fk_count) = 0,
			-- When PK count (l_cons_exist) > 0 and FK count (l_child_table_fk_count)= 0
			-- If both the count or any one of the count is coming to be 0, it means the table is having only PK in it or no PK and no Fk at all.
			-- In this case the table will be treated as a simple table where sbs_util.drop_partition is being called.
				CALL pub_admin.p_ctrl_log_event
					(
						in_event_constant			=> 'CO_DROP_PARTITION',
						in_table_name            	=> l_table_name,
						in_event_src_cd_location 	=> 'Procedure: DROP_PARTITION',
						in_event_statement       	=> 'sbs_util.drop_partition',
						in_event_dtl             	=> 'START DROP PARTITION',
						in_user_id               	=> l_audit_user
					);
				CALL sbs_util.drop_partition
					(
						p_owner 			=> l_schema_nm, 
						p_table_name 		=> l_table_name,
						p_partition_name 	=> l_partition_name
					);
				CALL pub_admin.p_ctrl_log_event
					(
						in_event_constant        	=> 'CO_DROP_PARTITION',
						in_table_name            	=> l_table_name,
						in_event_src_cd_location 	=> 'Procedure: DROP_PARTITION',
						in_event_statement       	=> 'sbs_util.drop_partition',
						in_event_dtl             	=> 'END DROP PARTITION',
						in_user_id               	=> l_audit_user
					);
			END IF;
		ELSE
			--partition name not exist in table
			RAISE EXCEPTION USING errcode = 50003;
		END IF;
	ELSE
			--schema name or table nameis NULL
		RAISE EXCEPTION USING ERRCODE = 50005;
	END IF;
EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(), table ' ,  in_table_name ,  ' not exists in schema ' ,  in_schema_nm,  CHR(10) , '. SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(), input parameter in_partition_name and in_partitition_value is NULL or empty ', CHR(10) , '. SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(), partition name ' ,  in_partition_name ,  '  not exist in table ' ,  in_table_name,  CHR(10) ,  '. SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(), Schema does not exists. ',  CHR(10) ,  '. SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50005' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(), input parameter passed for in_schema_nm or in_table_name is NULL or empty. ', CHR(10),  ' SQLERRM :- ' ,  SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50006' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(), dual value passed, please pass either partition name or parttion value. ', CHR(10),  ' SQLERRM :- ' ,  SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50007' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(), Child table has data, so Parent partition cannot be dropped ', CHR(10),  ' SQLERRM :- ' ,  SQLERRM),
				in_show_stack		=> TRUE 
			);
	WHEN SQLSTATE '50008' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- drop_partition(), Partition data type does not support. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE 
			);
	WHEN SQLSTATE '50000' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(), input parameter passed for in_partition_name or in_partition_value is empty string '''' it should be passed as NULL. ', CHR(10),  ' SQLERRM :- ' ,  SQLERRM),
				in_show_stack		=> TRUE 
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- drop_partition(), ', CHR(10),  ' SQLERRM :- ' ,  SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;