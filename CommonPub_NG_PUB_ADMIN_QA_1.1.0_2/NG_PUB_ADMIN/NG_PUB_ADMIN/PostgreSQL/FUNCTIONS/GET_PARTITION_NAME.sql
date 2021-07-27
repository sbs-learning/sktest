CREATE OR REPLACE FUNCTION pub_admin.get_partition_name
(
	in_schema_nm		CHARACTER VARYING,
	in_table_name		CHARACTER VARYING,
	in_part_value		CHARACTER VARYING,
	in_subpart_value	CHARACTER VARYING DEFAULT NULL::CHARACTER VARYING
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
		--in_part_value  : Partition value against whcih Partition name needs to be find out.
		--in_subpart_value : Sub Partition value against whcih Partition name needs to be find out. Not in use as of now
      -----------------------------------------------------------------------------------------------------------------------------
   --
   --------------------------------------------------------------------------------
   -- Name: get_partition_name
   --------------------------------------------------------------------------------
   --
   -- Description:  get_partition_name
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Kalyan Kumar	 CSPUBCC-4306	03/05/2021	get_partition_name() initial draft
   */

	v_rec							RECORD;
  	v_partitioning_type				VARCHAR (4000);
  	v_number_of_partition_columns	VARCHAR (4000);
	v_position_of_partition_columns	VARCHAR (4000);
	l_partitioning_type				VARCHAR (100);
	l_partition_column_name			VARCHAR (100);
	l_partition_column_data_type	VARCHAR (1000);
	l_part_high_value_in_number		VARCHAR (1000);
	l_part_high_value_in_date		VARCHAR (1000);
	l_partition_value_in_number		INTEGER;
	l_partition_value_in_date		DATE;
	l_part_high_value_in_date_from	DATE;
	l_part_high_value_in_date_to	DATE;
	l_part_high_value_in_number_from	INTEGER;
	l_part_high_value_in_number_to	INTEGER;
	
	---------
	max_array_length				INTEGER;
	l_part_high_value_in_varchar	VARCHAR (1000);
	l_original_partition_value		VARCHAR (1000);
  	l_partition_name				VARCHAR (100);
    sql_query						TEXT;
	co_range_partition				CONSTANT VARCHAR(100) :='RANGE';
	co_list_partition				CONSTANT VARCHAR(100) :='LIST';
	co_part_data_type_number		CONSTANT VARCHAR(100) :='NUMERIC';
	co_part_data_type_date			CONSTANT VARCHAR(100) :='DATE';
	co_part_data_type_integer		CONSTANT VARCHAR(100) :='INTEGER';
	co_part_data_type_varchar		CONSTANT VARCHAR(100) :='CHARACTER VARYING';
	co_part_data_type_char			CONSTANT VARCHAR(4) := 'CHAR';
	out_partition_name				VARCHAR(100) := NULL;
	
	l_schema_nm						CHARACTER VARYING(100);
	l_table_name					CHARACTER VARYING(100);
	l_part_value					CHARACTER VARYING(100);
	l_subpart_value					CHARACTER VARYING(100);
	l_format_call_stack				TEXT;
	l_table_exists					VARCHAR(1);

BEGIN

	l_schema_nm		:= 	LOWER(TRIM(in_schema_nm));
	l_table_name	:= 	LOWER(TRIM(in_table_name));
	l_part_value	:= 	TRIM(in_part_value);
	l_subpart_value	:= 	TRIM(in_subpart_value);

	IF l_schema_nm IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;
	IF l_table_name IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50002;
	END IF;
	IF l_part_value IS NULL THEN
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
				RAISE EXCEPTION USING ERRCODE = 50008;
        END;
    END IF;

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

	RAISE INFO ' Partition Type For the Provided % table  : %', l_table_name, v_partitioning_type;
	
	-- The value in v_position_of_partition_columns variable will give the column number in which partition was done.
	-- This value will give result as (3 4 5), removing spaces and adding ',' in between.
	-- This comma separated value will directly be used in next query to fetch the name and datatype of columns.
	v_position_of_partition_columns := REPLACE(v_position_of_partition_columns, ' ', ',');
	
	IF v_partitioning_type IS NOT NULL THEN
		BEGIN
			-- Query to fetch name and datatype of the partitioned columns.
			sql_query :=
			'SELECT column_name, data_type
			   FROM information_schema.columns 
			  WHERE TABLE_SCHEMA = '''||l_schema_nm||'''
                AND TABLE_NAME = '''||l_table_name||'''
			    AND ORDINAL_POSITION IN ('||v_position_of_partition_columns||')
			    LIMIT 1';
		EXECUTE sql_query INTO l_partition_column_name,l_partition_column_data_type;
		END;
	END IF;
	
	IF l_partition_column_name IS NULL OR l_partition_column_data_type IS NULL
	THEN
		RAISE EXCEPTION USING ERRCODE = 50005;
	END IF;
	
	RAISE INFO 'Partition table : %, Partition column name : %, Partition column data type : %', l_table_name, l_partition_column_name, l_partition_column_data_type;
	
	-- If Range partition Data Type supported :- NUMBER, DATE, INTEGER.
	IF v_partitioning_type = co_range_partition
		  AND upper(l_partition_column_data_type) IN (co_part_data_type_number, co_part_data_type_date, co_part_data_type_integer)   --Partition type 'RANGE'
	THEN
		BEGIN
			RAISE INFO 'Inside IF condition of RANGE Partition';
			FOR v_rec IN
				  (SELECT pg_class1.relname partition_name,
				          pg_get_expr(pg_class1.relpartbound, pg_class1.oid, TRUE) AS partition_expression
						  --pg_class2.relname table_name
				     FROM pg_class pg_class1,
					      pg_class pg_class2,
					      pg_inherits pg_inherits,
					      pg_namespace pg_namespace
				    WHERE pg_class1.relispartition IS TRUE
  				      AND pg_inherits.inhrelid = pg_class1.oid
				      AND pg_inherits.inhparent = pg_class2.oid
				      AND pg_class1.relnamespace = pg_namespace.oid
				      AND pg_namespace.nspname = l_schema_nm
				      AND pg_class2.relname = l_table_name
				  )
			LOOP
				RAISE INFO 'Original partition high value out put : %',v_rec.partition_expression;
				
				-- Check if the datatype of the column is DATE
				IF UPPER(l_partition_column_data_type) = co_part_data_type_date THEN
					RAISE INFO 'Inside DATE IF condition';
					l_partition_value_in_date := l_part_value;
					l_part_high_value_in_date_from := SPLIT_PART(SPLIT_PART(REPLACE(v_rec.partition_expression, '''',''''''), 'TO', 1),'FROM',2);
					l_part_high_value_in_date_to := SPLIT_PART(REPLACE(v_rec.partition_expression, '''',''''''), 'TO', 2);
					
					--IF l_part_high_value_in_date > l_partition_value_in_date
					IF ((l_partition_value_in_date >= l_part_high_value_in_date_from) AND
						(l_partition_value_in_date < l_part_high_value_in_date_to))
					THEN
						out_partition_name := v_rec.partition_name;
						EXIT;
					END IF;
				ELSIF UPPER(l_partition_column_data_type) = co_part_data_type_number 
				       OR UPPER(l_partition_column_data_type) = co_part_data_type_integer
					   THEN
					   	l_partition_value_in_number := l_part_value;
						RAISE INFO 'l_partition_value_in_number - %', l_partition_value_in_number;
					   	
						l_part_high_value_in_number_from := REPLACE(REPLACE(SPLIT_PART(SPLIT_PART(REPLACE(v_rec.partition_expression, '''',''''''), 'TO', 1),'FROM',2),')',''),'(','');
						RAISE INFO 'l_part_high_value_in_number_from - %', l_part_high_value_in_number_from;
						
						l_part_high_value_in_number_to := REPLACE(REPLACE(SPLIT_PART(REPLACE(v_rec.partition_expression, '''',''''''), 'TO', 2),')',''),'(','');
						RAISE INFO 'l_part_high_value_in_number_to - %', l_part_high_value_in_number_to;
						
					IF ((l_partition_value_in_number >= l_part_high_value_in_number_from) AND 
						(l_partition_value_in_number < l_part_high_value_in_number_to))
					THEN
						out_partition_name := v_rec.partition_name;
						EXIT;
					END IF;	
				ELSE
					-- We do not support  partition column data type  apart from Number ,Integer and Date data type  for the Range partition
					RAISE INFO 'Partition column data type not supported for the Range partition';
					RAISE EXCEPTION USING ERRCODE = 50006;
				END IF;
			END LOOP;
			END;

		ELSIF v_partitioning_type = co_list_partition
		  AND UPPER(l_partition_column_data_type) IN (co_part_data_type_char, co_part_data_type_varchar)
		  THEN
		  	BEGIN
		  	RAISE INFO 'PARTITION TYPE LIST :';
			FOR v_rec IN
				(SELECT pg_class1.relname partition_name,
						pg_get_expr(pg_class1.relpartbound, pg_class1.oid, true) AS partition_expression
				   FROM pg_class pg_class1,
						pg_class pg_class2,
						pg_inherits pg_inherits,
						pg_namespace pg_namespace
				  WHERE pg_class1.relispartition IS TRUE
					AND pg_inherits.inhrelid = pg_class1.oid
					AND pg_inherits.inhparent = pg_class2.oid
					AND pg_class1.relnamespace = pg_namespace.oid
					AND pg_namespace.nspname = l_schema_nm
					AND pg_class2.relname = l_table_name
				)
			LOOP
				RAISE INFO 'Original partition high value out put : %',v_rec.partition_expression;
				RAISE INFO 'Original partition name out put : %',v_rec.partition_name;

				l_part_high_value_in_varchar := SPLIT_PART(v_rec.partition_expression, 'IN', 2);
				l_part_high_value_in_varchar := RTRIM(LTRIM(l_part_high_value_in_varchar, '('),')');
				l_part_high_value_in_varchar := REPLACE(l_part_high_value_in_varchar, '(','');
				
				max_array_length := MAX(array_length(regexp_split_to_array(l_part_high_value_in_varchar, ','), 1));

				FOR I IN 1..max_array_length
				LOOP
					 l_original_partition_value := split_part(l_part_high_value_in_varchar,',', I);
					 RAISE INFO 'l_original_partition_value : %',l_original_partition_value;
					 IF TRIM (l_original_partition_value) = CHR(39)||l_part_value||CHR(39) THEN
					 	out_partition_name := v_rec.partition_name;
						EXIT;
					END IF;
				END LOOP;
			END LOOP;
		END;
	ELSE
		RAISE INFO 'We support partition RANGE/LIST TYPE';
		-- We support partiton by Range/List and subpartiton by List --Ref#15
		--If table Partition by Range then Data Type should be ' NUMBER','DATE','INTERGER'
		--Subpartition should be list  and data type should be VARCHAR2,VARCHAR,CHAR
		--RAISE EXCEPTION 'Partition data type does not support ';
		RAISE EXCEPTION USING ERRCODE = 50006;
	END IF;

	IF out_partition_name IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50007;
		--If we do not get a match corresponding to  the passed parameters
		--raise an exception
	ELSE
		RETURN out_partition_name;
	END IF;

EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_name(), Schema Name cannot be null or empty. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_name(), Table Name cannot be null or empty. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_name(), Partition value cannot be null or empty. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_name(), Table ',in_table_name, ' is not partitioned. ',CHR(10),' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50005' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_name(), No columns were found on which Partiotion is done. Partitions could have been done on Function based columns. Currently we are not supporting that. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50006' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_name(), Partition data type does not support. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50007' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_name(), The supplied value doesnt fit to any partition. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50008' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_name(), Schema name or Table name does not exists.  Schema name passed - ',in_schema_nm, ', Table name passed - ',in_table_name, CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_partition_name(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;