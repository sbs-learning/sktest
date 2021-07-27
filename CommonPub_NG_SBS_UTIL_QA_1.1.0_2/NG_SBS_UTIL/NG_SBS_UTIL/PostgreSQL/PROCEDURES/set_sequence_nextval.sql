CREATE OR REPLACE PROCEDURE sbs_util.set_sequence_nextval
(
	in_schema_name		CHARACTER VARYING,
	in_sequence_name	CHARACTER VARYING,
	in_target_value		NUMERIC,
	in_echo_sql			BOOLEAN DEFAULT FALSE
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE

   -----------------------------------------------------------------------------
   -- Name :set_sequence_nextval                                             
   -----------------------------------------------------------------------------
   -- Description : Sets a sequence to have the designated nextval 
   -- Parameters: in_schema_name, in_sequence_name, in_target_value, in_echo_sql
   ------------------------------------------------------------------------------
   -- RefNo Name             JIRA NO       Date     Description of change                      
   -- ----- ---------------- -------- ------------ -------------------------------
   -- 1     Akshay       CSPUBCC-4312   04/01/2021  Initial Version                         
   -- 2	    Akshay	     CSPUBCC-4383	04/14/2021	Added variable l_schema_name, l_sequence_name
   --                                               and lower function in query
   -- 3     Akshay       CSPUBCC-4533   06/03/2021  Handle empty string for input parameters.   
   ------------------------------------------------------------------------------

    l_sql                  VARCHAR(500);
    l_original_increment   NUMERIC;
    l_original_nextval     NUMERIC;
    l_adjusting_increment  NUMERIC;
    l_throwaway_nextval    NUMERIC;
    l_next_value           NUMERIC;
    l_last_seq             NUMERIC;
    l_min_value            NUMERIC;
    l_max_value            NUMERIC;
    l_cache_value          NUMERIC;
    l_minvalue             NUMERIC;				
    l_maxvalue             NUMERIC;
    l_schema_name          VARCHAR(100);
    l_sequence_name  	   VARCHAR(100);
    l_format_call_stack	   TEXT;
	
BEGIN
	l_schema_name 	:= LOWER(TRIM(in_schema_name));
	l_sequence_name := LOWER(TRIM(in_sequence_name));

	IF l_schema_name IS NULL OR l_schema_name = '' OR l_sequence_name IS NULL OR l_sequence_name = '' OR in_target_value IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50004;
	END IF;

	BEGIN
		SELECT min_value, max_value
		  INTO STRICT l_minvalue, l_maxvalue
		  FROM pg_catalog.pg_sequences
		 WHERE schemaname = l_schema_name
		   AND sequencename = l_sequence_name;
	EXCEPTION
		WHEN NO_DATA_FOUND THEN
			RAISE EXCEPTION USING ERRCODE = 50005;
	END;

	IF(in_target_value <= l_minvalue) THEN
		RAISE INFO 'Please provide in_target_value input higher then the assigned minvalue of sequence';
		RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;

	IF(in_target_value > l_maxvalue) THEN
		RAISE INFO 'You can not set sequence nextval greater then its maxvalue';
		RAISE EXCEPTION USING ERRCODE = 50002;
	END IF;

    BEGIN
		l_sql := 'SELECT NEXTVAL('''||l_schema_name||'.'||l_sequence_name||''')';

		IF in_echo_sql THEN
			RAISE INFO 'Executing: %',l_sql;
		END IF;

		EXECUTE l_sql INTO l_original_nextval;

		IF in_echo_sql THEN
			RAISE INFO 'Result: %',l_original_nextval;
		END IF;

	EXCEPTION
        WHEN SQLSTATE '2200H' THEN
			l_sql := 'SELECT CURRVAL('''||l_schema_name||'.'||l_sequence_name||''')';

		IF in_echo_sql THEN
			RAISE INFO 'Executing: %',l_sql;
		END IF;

		EXECUTE l_sql into l_original_nextval;
		 
		IF in_echo_sql THEN
			RAISE INFO 'Result: %',l_original_nextval;
		END IF;
    END;

	SELECT min_value, last_value, increment_by, max_value, cache_size
	  INTO l_min_value, l_last_seq, l_original_increment, l_max_value, l_cache_value
      FROM pg_catalog.pg_sequences
     WHERE schemaname = l_schema_name
       AND sequencename = l_sequence_name;

	RAISE INFO 'last number: %, target value : %, l_original_nextval : %', l_last_seq, in_target_value, l_original_nextval;

	l_adjusting_increment := in_target_value - l_original_nextval - l_original_increment;
    l_next_value  := l_last_seq + l_adjusting_increment;

	RAISE INFO 'l_adjusting_increment: %, l_min_value: %, l_last_seq: %, l_next_value: % , l_max_value: %, l_cache_value: %',
	l_adjusting_increment, l_min_value, l_last_seq, l_next_value, l_max_value, l_cache_value;

	IF(l_adjusting_increment = 0) THEN
		RAISE INFO '%sequence adjust increment: % is zero and can not be used for ALTER SEQUENCE', l_sequence_name, l_adjusting_increment;
		RETURN;
    END IF;

	IF  l_next_value >= l_min_value THEN
		IF l_cache_value > 1 THEN
			l_sql := 'ALTER SEQUENCE '||l_schema_name||'.'||l_sequence_name||' CACHE 2 '||
					 ' INCREMENT BY '||l_adjusting_increment;

			IF in_echo_sql THEN
				RAISE INFO 'Executing: %',l_sql;
			END IF;

			EXECUTE l_sql;

			l_sql := 'SELECT NEXTVAL('''||l_schema_name||'.'||l_sequence_name||''')';

			IF in_echo_sql THEN
				RAISE INFO 'Executing: %',l_sql;
			END IF;

			EXECUTE l_sql INTO l_throwaway_nextval;

			IF in_echo_sql THEN
				RAISE INFO 'Result: %',l_throwaway_nextval;
			END IF;

			l_sql := 'ALTER SEQUENCE '||l_schema_name||'.'||l_sequence_name||'  CACHE   '|| l_cache_value || 
                     ' INCREMENT BY '||l_original_increment;

			IF in_echo_sql THEN
				RAISE INFO 'Executing: %',l_sql;
            END IF;

			EXECUTE l_sql;

		ELSE
			l_sql := 'ALTER SEQUENCE '||l_schema_name||'.'||l_sequence_name||
					 ' INCREMENT BY '||l_adjusting_increment;

			IF in_echo_sql THEN
				RAISE INFO 'Executing: %',l_sql;
			END IF;

            EXECUTE l_sql;

			l_sql := 'SELECT NEXTVAL('''||l_schema_name||'.'||l_sequence_name||''')';

			IF in_echo_sql THEN
				RAISE INFO 'Executing: %',l_sql;
			END IF;

			EXECUTE l_sql INTO l_throwaway_nextval;

			IF in_echo_sql THEN
				RAISE INFO 'Result: %',l_throwaway_nextval;
			END IF;

			l_sql := 'ALTER SEQUENCE '||l_schema_name||'.'||l_sequence_name||
					 ' INCREMENT BY '||l_original_increment;

			IF in_echo_sql THEN
				RAISE INFO 'Executing: %',l_sql;
			END IF;

            EXECUTE l_sql;

		END IF;

	ELSE

		RAISE info ' %sequence adjust increment: % is less then min_value of sequence : %', l_sequence_name, l_adjusting_increment, l_min_value;
		RAISE EXCEPTION USING errcode = 50003;

	END IF;

EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- set_sequence_nextval(),parameter in_target_value input is less then the assigned minvalue of sequence. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
	  	CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- set_sequence_nextval(),parameter in_target_value can not set sequence nextval greater then its maxvalue. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
	  	CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- set_sequence_nextval(), sequence adjust increment ' ,  l_adjusting_increment , ' is less then min_value of sequence ' , l_min_value , CHR(10) , ' SQLERRM:- ' , SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50004' THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
	  	CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in procedure :- set_sequence_nextval(), Either of the input parameters is having NULL or Empty value. ', ' in_schema_name - ', in_schema_name, ', in_sequence_name - ', in_sequence_name, ', in_target_value - ', in_target_value , CHR(10) , 'SQLERRM:- ' , SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50005' THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;	  
	  	CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in procedure :- set_sequence_nextval(), Either schema name or sequence name does not exists. ', ' in_schema_name - ', in_schema_name, ', in_sequence_name - ', in_sequence_name, CHR(10) , 'SQLERRM:- ' , SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- set_sequence_nextval(). ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;