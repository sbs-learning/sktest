CREATE OR REPLACE FUNCTION sbs_util.get_base36(
	in_dec NUMERIC,
	in_width INTEGER DEFAULT 0)
    RETURNS CHARACTER VARYING
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
 --------------------------------------------------------------------------
	-- Purpose : Convert any decimal only upto base36 with width as defined in in_width parameter.
    -- Parameter Details
	--in_dec : Decimal number for conversion
	--in_width:    Width of converted number
      -----------------------------------------------------------------------------------------------------------------------------
   --
   --------------------------------------------------------------------------------
   -- Name: get_base36
   --------------------------------------------------------------------------------
   --
   -- Description:  get_base
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Tamheed Khan CSPUBCC-4307	04/05/2021	[NextGen] SBS_UTIL Feature for get_audit, get_base, get_hash
   --   2   Akshay       CSPUBCC-4383   04/19/2021   Added exception block in the query.
   
	l_base36   				VARCHAR DEFAULT NULL;
    l_format_call_stack		TEXT;
	co_base36 				CONSTANT INTEGER :=36;
BEGIN

	IF (in_dec IS NULL)
	THEN
		RAISE EXCEPTION USING ERRCODE = 50002;
	ELSIF in_width IS NULL THEN
		RAISE EXCEPTION USING ERRCODE = 50003;
	ELSIF in_width > 36 THEN
		RAISE EXCEPTION USING ERRCODE = 50004;
	END IF; 
 
	l_base36 := sbs_util.get_base (in_dec => in_dec, in_base => co_base36); -- To convert the decimal number only in base36z
	IF in_width = 0 THEN--If user didn't demands for specific width, returning converted number as original.
		RETURN l_base36;
	ELSIF in_width < LENGTH (l_base36)--If user desired width is less then the converted one, then error raised
	THEN
		RAISE info 'User passed width is % which is lesser than converted number width %. Please pass increased width, minimum % %',in_width,LENGTH(l_base36),LENGTH(l_base36),CHR(46);
		RAISE EXCEPTION USING ERRCODE = 50001;
	ELSE
		RETURN LPAD (l_base36, in_width::INTEGER, 0::VARCHAR);--Padding 0 to left side of number to match user expected width
	END IF;
EXCEPTION
    WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		(
			in_error_stack		=> l_format_call_stack,
			in_error_code		=> SQLSTATE, 
			in_error_message	=> CONCAT('Error in procedure :- sbs_util.get_base36(), User passed width is lesser than converted number width. ', CHR(10), ' SQLERRM:- ', SQLERRM),
			in_show_stack		=> TRUE
		);
    WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		(
			in_error_stack		=> l_format_call_stack,
			in_error_code		=> SQLSTATE, 
			in_error_message	=> CONCAT('Error in procedure :- sbs_util.get_base36(), The value in the parameter in_dec cannot have null or empty value ', CHR(10), ' SQLERRM:- ', SQLERRM),
			in_show_stack		=> TRUE
		);
    WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		(
			in_error_stack		=> l_format_call_stack,
			in_error_code		=> SQLSTATE, 
			in_error_message	=> CONCAT('Error in procedure :- sbs_util.get_base36(), The value in the parameter in_width cannot have null or empty value ', CHR(10), ' SQLERRM:- ', SQLERRM),
			in_show_stack		=> TRUE
		);
    WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		(
			in_error_stack		=> l_format_call_stack,
			in_error_code		=> SQLSTATE, 
			in_error_message	=> CONCAT('Error in procedure :- sbs_util.get_base36(), User passed width parameter value is greater than 36. ', CHR(10), ' SQLERRM:- ', SQLERRM),
			in_show_stack		=> TRUE
		);
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		(
			in_error_stack		=> l_format_call_stack, 
			in_error_code		=> SQLSTATE, 
			in_error_message	=> CONCAT(' Error in procedure :- sbs_util.get_base36(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
			in_show_stack		=> TRUE
		);
END;
$BODY$;