CREATE OR REPLACE FUNCTION sbs_util.get_base
(
	in_dec 		NUMERIC,
	in_base 	INTEGER
)
    RETURNS CHARACTER VARYING
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
 --------------------------------------------------------------------------
	-- Purpose : Support converting integers to a base of your choice--only upto base 36
    -- Parameter Details
	--in_dec : Decimal number for conversion
	--in_base  : Base to which number conversion require.
      -----------------------------------------------------------------------------------------------------------------------------
   --
   --------------------------------------------------------------------------------
   -- Name: get_base
   --------------------------------------------------------------------------------
   --
   -- Description:  get_base
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Tamheed Khan CSPUBCC-4307	04/05/2021	[NextGen] SBS_UTIL Feature for get_audit, get_base, get_hash
   --   2   Akshay       CSPUBCC-4383   04/19/2021   Added exception block in the query.
   
	l_str				VARCHAR DEFAULT NULL;
    l_num				NUMERIC DEFAULT in_dec;
    l_hex				VARCHAR DEFAULT '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    l_invalid_decimal  	BOOLEAN	:=FALSE;
    l_invalid_base     	BOOLEAN	:=FALSE;
    l_format_call_stack	TEXT;
 BEGIN
	IF (TRUNC (in_dec) <> in_dec OR COALESCE(in_dec,-1) < 0 OR in_dec IS NULL ) THEN
		l_invalid_decimal := TRUE;
	END IF;

	IF (in_base NOT BETWEEN 2 AND 36 OR (in_base IS NULL)) THEN--This package supports base conversions from 2 to 36 only.
		l_invalid_base := TRUE;
	END IF;

    IF l_invalid_decimal AND l_invalid_base THEN
		RAISE info 'Invalid decimal or base';
		RAISE EXCEPTION USING errcode = 50001;
	ELSIF l_invalid_decimal THEN
		RAISE info 'Pass a valid decimal number to convert.';
		RAISE EXCEPTION USING errcode = 50002;
	ELSIF l_invalid_base THEN
		RAISE info 'Pass a valid base (between 2 to 36) to convert.';
		RAISE EXCEPTION USING errcode = 50003;
	END IF; -- All possible user exception checked here

    LOOP
		l_str := SUBSTR(l_hex,(MOD(l_num, in_base))::INTEGER + 1,1)||COALESCE(l_str,'');
		l_num := TRUNC (l_num / in_base);
		EXIT WHEN (l_num = 0);
	END LOOP;
    RETURN l_str;
EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		(
			in_error_stack		=> l_format_call_stack,
			in_error_code		=> SQLSTATE, 
			in_error_message	=> CONCAT('Error in function :- get_base(), Invalid decimal or base. ', CHR(10), ' SQLERRM:- ', SQLERRM),
			in_show_stack		=> TRUE
		);
	WHEN SQLSTATE '50002' THEN  
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		(
			in_error_stack		=> l_format_call_stack,
			in_error_code		=> SQLSTATE, 
			in_error_message	=> CONCAT('Error in function :- get_base(), Invalid value is passed for conversion. ', CHR(10), ' SQLERRM:- ', SQLERRM),
			in_show_stack		=> TRUE
		);
	WHEN SQLSTATE '50003' THEN  
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		(
			in_error_stack		=> l_format_call_stack,
			in_error_code		=> SQLSTATE,
			in_error_message	=> CONCAT('Error in function :- get_base(), Invalid value is passed for base. ', CHR(10), ' SQLERRM:- ', SQLERRM),
			in_show_stack		=> TRUE
		);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		(
			in_error_stack		=> l_format_call_stack, 
			in_error_code		=> SQLSTATE, 
			in_error_message	=> CONCAT(' Error in function :- get_base(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
			in_show_stack		=> TRUE
		);
END;
$BODY$;