CREATE OR REPLACE FUNCTION sbs_util.get_numeric_version
(
	in_version CHARACTER VARYING
)
RETURNS numeric
LANGUAGE 'plpgsql'
COST 100
VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    -------------------------------------------------------------------------------------
    -- Name : get_numeric_version
    -------------------------------------------------------------------------------------
    -- Description : To get the numeric value of any version.
    -- Parameters : in_version
	----------------------------------------------------------------------------------
    -- RefNo Name            JIRA NO 		   Date           Description of change
    -- ----- ---------------- -------- ---------------------------------------------
    -- 	1	Akshay  	    CSPUBCC-4401	04/19/2021     	  Added exception block in the query.
    -------------------------------------------------------------------------------------
    s_csv_version			VARCHAR(100);
    s_alpha_version			VARCHAR(2);
	s_numeric_version		NUMERIC;
	s_numeric_version_c		VARCHAR(3);
    nversionval				NUMERIC;
    ninc					NUMERIC;
    nalpha_version			NUMERIC;
    l_comma_count			NUMERIC;
    l_to_the_power_num		NUMERIC;
    l_to_the_power_alph		NUMERIC;
    l_s_var_version			VARCHAR(100);
    L_MAX_VALUE				NUMERIC;
    l_num_version			VARCHAR(4000);
	a_num					NUMERIC;
	l_format_call_stack		TEXT;
	
BEGIN
    IF TRIM(BOTH ' ' FROM in_version) IS NULL THEN
        RAISE INFO 'Invalid input version passed - %', in_version;
		RAISE EXCEPTION USING errcode = 50001;
    END IF;
	
	l_num_version := SUBSTRING(in_version FROM '[[:digit:]].*');
	RAISE INFO 'l_num_version - %', l_num_version;
	
    nversionval         := 0;
    l_to_the_power_num  := 14;
    l_to_the_power_alph := 12;
    
	s_csv_version := REPLACE(in_version, '.', ',');
	RAISE INFO 's_csv_version - %',s_csv_version;
	
    l_comma_count := LENGTH (s_csv_version) - LENGTH(REPLACE(s_csv_version,',',''))+1;
	RAISE INFO 'l_comma_count - %',l_comma_count;
	
    FOR  rec in 1..l_comma_count
    LOOP

		l_s_var_version := split_part(s_csv_version,',',rec);
		
		s_numeric_version := COALESCE(SUBSTRING(SUBSTRING(l_s_var_version,'[^,]+'),'[0-9]+'),'0');
		
		RAISE INFO 's_numeric_version - %',s_numeric_version;

		s_alpha_version := COALESCE(SUBSTRING(SUBSTRING(l_s_var_version,'[^,]+'),'[a-z]+'),' ');

		RAISE INFO 's_alpha_version - %',s_alpha_version;

		IF s_alpha_version IS NOT NULL THEN
    		nalpha_version := ASCII (upper(s_alpha_version)) - 32; --Lowercase 'a' is ascii 97
	    
			RAISE INFO 'nalpha_version - %',nalpha_version;
		ELSE
        	nalpha_version := 0;
			RAISE INFO 'nalpha_version - %',nalpha_version;
    	END IF;
		
		s_numeric_version_c := s_numeric_version;
		
         --Prevent values which overlap
        IF LENGTH (s_numeric_version_c) > 2 THEN
			RAISE INFO 'sub_version greater than 99. %', l_s_var_version;
			RAISE EXCEPTION USING errcode = 50002;
        END IF;
		
        ninc := (s_numeric_version* POWER (10,l_to_the_power_num)
                 +
                 nalpha_version
                 * POWER (10, l_to_the_power_alph)); --Add alphabet position number (1-26), one power of ten lower.
		
		RAISE INFO 'ninc - %', ninc;
		RAISE INFO 'Sub Version - %, Sub_Version N - %, Sub_Version A - %, Level - %, Inc - %',
		           l_s_var_version, s_numeric_version, s_alpha_version, rec, ninc;
        nversionval := nversionval + ninc;
		l_to_the_power_num:=l_to_the_power_num-4;
		l_to_the_power_alph:=l_to_the_power_alph-4;
    END LOOP;
    
	IF(SUBSTR(TO_CHAR(nversionval,'FM999999999999999999'),strpos(TO_CHAR(nversionval,'FM999999999999999999'),'.')+1,2))<>'00' THEN
		IF (SUBSTR(TO_CHAR(nversionval,'FM999999999999999999'),strpos(TO_CHAR(nversionval,'FM999999999999999999'),'.')+1,2))='10' THEN
			RETURN ROUND(nversionval,1);
		ELSE
			RETURN ROUND(nversionval,2);
		END IF;
	ELSE
		RETURN TRUNC(nversionval);
	END IF;
EXCEPTION
    WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in function :- get_numeric_version(),parameter in_version not accept null value. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT('Error in function :- get_numeric_version(),sub_version is greater than 99. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- get_numeric_version(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;