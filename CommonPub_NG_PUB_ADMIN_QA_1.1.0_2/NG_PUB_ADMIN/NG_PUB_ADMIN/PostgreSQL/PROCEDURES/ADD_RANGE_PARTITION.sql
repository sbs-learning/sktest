CREATE OR REPLACE PROCEDURE pub_admin.add_range_partition(
	in_schema_name     CHARACTER VARYING,
	in_table_name      CHARACTER VARYING,
	in_partition_name  CHARACTER VARYING,
	in_partition_value CHARACTER VARYING)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
   ------------------------------------------------------------------------------------------------
   --Name : ADD_RANGE_PARTITION
   ------------------------------------------------------------------------------------------------
   -- Description : This procedure will create partition on existing Range Partitioned Table.
   --               It will support INTEGER type.
   --
   -- Parameters : in_schema_name, in_table_name, in_partition_name, in_partition_value
   --
   -----------------------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		   Date           Description of change
   -- ----- ---------------- -------- ------------------------------------------------------------
   -- 	1	Akshay  	    CSPUBCC-4355	  6/9/2021       add_range_partition() initial draft
   -----------------------------------------------------------------------------------------------
   
   l_schema_name                     VARCHAR(100);
   l_table_name                      VARCHAR(100);
   l_partition_name                  VARCHAR(100);
   l_partition_value                 VARCHAR(100);
   l_range_part_exist                SMALLINT;
   v_rec_val                         RECORD;
   l_partition_name_exist            SMALLINT;
   v_sql                             VARCHAR (4000);
   v_partition_val1                  VARCHAR;
   l_part_low_value_in_number_from   VARCHAR;
   l_part_high_value_in_number_to    VARCHAR;
   l_part_low_value_in_number_from1  INTEGER;
   l_part_high_value_in_number_to1   INTEGER;
   l_part_expression                 VARCHAR;
   l_part_high_value_to              VARCHAR;
   l_part_high_value_to1             INTEGER;
   l_audit_user                      VARCHAR(100);
   l_format_call_stack               TEXT;
   
BEGIN
      l_schema_name:= LOWER(TRIM(in_schema_name));
	  l_table_name:= LOWER(TRIM(in_table_name));
	  l_partition_name:= LOWER(TRIM(in_partition_name));
	  l_partition_value:= TRIM(in_partition_value);
	  l_audit_user := sbs_util.get_audit_user();

   -- Check whether Table is already Range Partitioned or Not. If Not, No partition will be created.
         SELECT COUNT(1) INTO l_range_part_exist
				     FROM pg_class pg_class1,
					      pg_class pg_class2,
					      pg_inherits pg_inherits,
					      pg_namespace pg_namespace
				    WHERE pg_class1.relispartition IS TRUE
  				      AND pg_inherits.inhrelid = pg_class1.oid
				      AND pg_inherits.inhparent = pg_class2.oid
				      AND pg_class1.relnamespace = pg_namespace.oid
				      AND pg_namespace.nspname = l_schema_name
				      AND pg_class2.relname = l_table_name;

            IF l_range_part_exist = 0
              THEN
			    RAISE EXCEPTION USING ERRCODE = 50005;
            END IF;

        FOR v_rec_val
                IN (SELECT pg_get_expr(pg_class1.relpartbound, pg_class1.oid, TRUE) AS partition_expression
				     FROM pg_class pg_class1,
					      pg_class pg_class2,
					      pg_inherits pg_inherits,
					      pg_namespace pg_namespace
				    WHERE pg_class1.relispartition IS TRUE
  				      AND pg_inherits.inhrelid = pg_class1.oid
				      AND pg_inherits.inhparent = pg_class2.oid
				      AND pg_class1.relnamespace = pg_namespace.oid
				      AND pg_namespace.nspname = l_schema_name
				      AND pg_class2.relname = l_table_name
                   )
            LOOP
                      v_partition_val1 := v_rec_val.partition_expression;
					  RAISE INFO 'v_partition_val1 : %',v_partition_val1;
					  
		         l_part_low_value_in_number_from := SPLIT_PART(SPLIT_PART(REPLACE(v_partition_val1, '',''), 'TO', 1),'FROM',2);
		         RAISE INFO 'l_part_low_value_in_number_from : %',l_part_low_value_in_number_from;
		         l_part_low_value_in_number_from1 := TRIM(REPLACE(REPLACE(l_part_low_value_in_number_from, '(''', ''), ''')', ''));
	             RAISE INFO 'l_part_low_value_in_number_from1 : %',l_part_low_value_in_number_from1;
		         
		         l_part_high_value_in_number_to := SPLIT_PART(REPLACE(v_partition_val1, '',''), 'TO', 2);
		         RAISE INFO 'l_part_high_value_in_number_to : %',l_part_high_value_in_number_to;
	             l_part_high_value_in_number_to1 := TRIM(REPLACE(REPLACE(l_part_high_value_in_number_to, '(''', ''), ''')', ''));
	             RAISE INFO 'l_part_high_value_in_number_to1 : %',l_part_high_value_in_number_to1;
		
		        IF (l_partition_value::INTEGER = l_part_low_value_in_number_from1)
		        	THEN
		        		RAISE EXCEPTION USING ERRCODE = 50007;
						
		        ELSIF (l_partition_value::INTEGER = l_part_high_value_in_number_to1)
		        	THEN
		        		RAISE EXCEPTION USING ERRCODE = 50008;
						
				ELSIF ((l_partition_value::INTEGER > l_part_low_value_in_number_from1) AND
		        	(l_partition_value::INTEGER < l_part_high_value_in_number_to1))
		        	THEN
		        		RAISE EXCEPTION USING ERRCODE = 50006;
		        END IF;
            END LOOP;
			
			SELECT COUNT(1) INTO l_partition_name_exist
				     FROM pg_class pg_class1,
					      pg_class pg_class2,
					      pg_inherits pg_inherits,
					      pg_namespace pg_namespace
				    WHERE pg_class1.relispartition IS TRUE
  				      AND pg_inherits.inhrelid = pg_class1.oid
				      AND pg_inherits.inhparent = pg_class2.oid
				      AND pg_class1.relnamespace = pg_namespace.oid
				      AND pg_namespace.nspname = l_schema_name
				      AND pg_class2.relname = l_table_name
					  AND pg_class1.relname = l_partition_name;

            IF l_partition_name_exist > 0
                 THEN
				    RAISE EXCEPTION USING ERRCODE = 50009;
				ELSE
				  SELECT MAX(pg_get_expr(pg_class1.relpartbound, pg_class1.oid, TRUE))
				     INTO STRICT l_part_expression
				     FROM pg_class pg_class1,
					      pg_class pg_class2,
					      pg_inherits pg_inherits,
					      pg_namespace pg_namespace
				    WHERE pg_class1.relispartition IS TRUE
  				      AND pg_inherits.inhrelid = pg_class1.oid
				      AND pg_inherits.inhparent = pg_class2.oid
				      AND pg_class1.relnamespace = pg_namespace.oid
				      AND pg_namespace.nspname = l_schema_name
				      AND pg_class2.relname = l_table_name;
					  
					  RAISE INFO 'v_partition_val_max : %',l_part_expression;
				  
					 l_part_high_value_to := SPLIT_PART(REPLACE(l_part_expression, '',''), 'TO', 2);
		            RAISE INFO 'l_part_high_value_to : %',l_part_high_value_to;
	                 l_part_high_value_to1 := TRIM(REPLACE(REPLACE(l_part_high_value_to, '(''', ''), ''')', ''));
	                RAISE INFO 'l_part_high_value_to1 : %',l_part_high_value_to1;
					
				IF(l_partition_value::INTEGER > l_part_high_value_to1) THEN
						v_sql :=
                           'CREATE TABLE '
                        || l_schema_name
                        || '.'
                        || l_partition_name
                        || ' PARTITION OF '
						|| l_schema_name
                        || '.'
                        || l_table_name
                        || ' FOR VALUES FROM '
                        || '('
                        || l_part_high_value_to1
                        || ')'
						|| ' TO '
						|| '('
                        || l_partition_value::INTEGER
                        || ')';
						
                        RAISE INFO 'v_sql: %' , v_sql;
						EXECUTE v_sql;
			    END IF;
            END IF;
			
       	CALL pub_admin.p_ctrl_log_event
						(
							in_event_constant			=> 'CO_ADD_RANGE_PARTITION',
							in_table_name            	=> l_table_name,
							in_event_src_cd_location 	=> 'Procedure: ADD_RANGE_PARTITION',
							in_event_statement       	=> v_sql,
							in_event_dtl             	=> 'NEW RANGE PARTITION ADDED',
							in_user_id               	=> l_audit_user
						);
 EXCEPTION
 	WHEN SQLSTATE '50005' THEN
	  GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in procedure :- add_range_partition(), Table is not range partitioned so, no new partition will be created. Table name passed - ',l_table_name, CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack 		=> TRUE
			);
	WHEN SQLSTATE '50006' THEN
	  GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in procedure :- add_range_partition(), Partition value lies between lower value and higher value. Partition value - ',l_partition_value, ', lower value - ',l_part_low_value_in_number_from1, ', higher value - ',l_part_high_value_in_number_to1, CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack 		=> TRUE
			);
	WHEN SQLSTATE '50007' THEN
	  GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in procedure :- add_range_partition(), Partition value already exist for lower value - ',l_partition_value, ', in table - ',l_table_name, CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack 		=> TRUE
			);
	WHEN SQLSTATE '50008' THEN
	  GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in procedure :- add_range_partition(), Partition value already exist for higher value - ',l_partition_value, ', in table - ',l_table_name, CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack 		=> TRUE
			);
 	WHEN SQLSTATE '50009' THEN
	  GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in procedure :- add_range_partition(), Partition Name - ',l_partition_name, ', in table - ',l_table_name, ', already exists. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack 		=> TRUE
			);
	WHEN OTHERS THEN
	  GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- add_range_partition(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;