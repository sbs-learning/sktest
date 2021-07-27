CREATE OR REPLACE FUNCTION sbs_util.get_hash_text
(
	IN in_string TEXT
)
RETURNS TEXT
LANGUAGE 'plpgsql'    
AS $BODY$
DECLARE
 --------------------------------------------------------------------------
	-- Purpose : To calculate hash of given Text.
    -- Parameter Details
	--in_string : text  to convert
      -----------------------------------------------------------------------------------------------------------------------------
   --
   --------------------------------------------------------------------------------
   -- Name: get_hash_text
   --------------------------------------------------------------------------------
   --
   -- Description:  get_hash_text
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Tamheed Khan CSPUBCC-4307	04/06/2021	[NextGen] SBS_UTIL Feature for get_audit, get_base, get_hash
   --   2   Kalyan Kumar CSPUBCC-4568   06/09/2021  Added Replace method to handle slash \ coming in the value of the input parameter.
   
	l_format_call_stack		TEXT;
BEGIN
	in_string := REPLACE(in_string, '\', '\\');
	RETURN ENCODE(sha256(in_string::bytea), 'hex');
EXCEPTION
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
		(
			in_error_stack		=> l_format_call_stack, 
			in_error_code		=> SQLSTATE, 
			in_error_message	=> CONCAT(' Error in function :- get_hash_text(), ', CHR(10), ' SQLERRM: ', SQLERRM),
			in_show_stack		=> TRUE
		);
END;
$BODY$;