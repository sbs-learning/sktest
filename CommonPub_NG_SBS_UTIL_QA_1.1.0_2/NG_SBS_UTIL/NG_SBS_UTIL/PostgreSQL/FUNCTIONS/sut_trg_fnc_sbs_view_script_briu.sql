CREATE OR REPLACE FUNCTION sbs_util.SUT_TRG_FNC_SBS_VIEW_SCRIPT_BRIU()
RETURNS TRIGGER 
LANGUAGE 'plpgsql'
AS
$$
DECLARE
	l_format_call_stack TEXT;
BEGIN

	--IF INSERTING THEN
	IF (TG_OP = 'INSERT') THEN
		NEW.rcrd_create_user_id := sbs_util.get_audit_user();
		NEW.rcrd_create_ip 		:= inet_client_addr();
		NEW.rcrd_create_ts 		:= CURRENT_TIMESTAMP;
	--IF UPDAING THEN
	ELSIF (TG_OP = 'UPDATE') THEN
		NEW.rcrd_updt_user_id 	:= sbs_util.get_audit_user();
		NEW.rcrd_updt_ip 		:= inet_client_addr();
		NEW.rcrd_updt_ts 		:= CURRENT_TIMESTAMP;
	END IF;
	RETURN NEW;
EXCEPTION
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in Procedure :- SUT_TRG_FNC_SBS_VIEW_SCRIPT_BRIU(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack 		=> TRUE
			);
END;
$$;
