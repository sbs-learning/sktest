CREATE OR REPLACE FUNCTION pub_admin.PBA_TRG_FNC_EVENT_NAME_ARIUD()
  RETURNS TRIGGER
  AS
$$
DECLARE
	r_old_hstry pub_admin.pba_event_name_hst_t%ROWTYPE;
	r_new_hstry pub_admin.pba_event_name_hst_t%ROWTYPE;
	l_format_call_stack TEXT;
BEGIN

	r_old_hstry.event_name_id				:= OLD.event_name_id;
	r_old_hstry.event_constant				:= OLD.event_constant;
	r_old_hstry.event_name					:= OLD.event_name;
	r_old_hstry.event_name_desc				:= OLD.event_name_desc;
	r_old_hstry.rcrd_updt_user_id           := OLD.rcrd_updt_user_id;
	r_old_hstry.rcrd_updt_ip                := OLD.rcrd_updt_ip;
	r_old_hstry.rcrd_updt_ts                := OLD.rcrd_updt_ts;

	r_old_hstry.rcrd_create_user_id			:= OLD.rcrd_create_user_id;
	r_old_hstry.rcrd_create_ts				:= OLD.rcrd_create_ts;
	r_old_hstry.rcrd_create_ip				:= OLD.rcrd_create_ip;
	
	r_old_hstry.hst_rcrd_create_user_id 	:= sbs_util.get_audit_user();
	r_old_hstry.hst_rcrd_create_ip      	:= inet_client_addr();
	r_old_hstry.hst_rcrd_create_ts      	:= CURRENT_TIMESTAMP;

	r_new_hstry.event_name_id				:= NEW.event_name_id;
	r_new_hstry.event_constant				:= NEW.event_constant;
	r_new_hstry.event_name					:= NEW.event_name;
	r_new_hstry.event_name_desc				:= NEW.event_name_desc;
	r_new_hstry.rcrd_updt_user_id           := NEW.rcrd_updt_user_id;
	r_new_hstry.rcrd_updt_ip                := NEW.rcrd_updt_ip;
	r_new_hstry.rcrd_updt_ts                := NEW.rcrd_updt_ts;
	
	r_new_hstry.rcrd_create_user_id			:= NEW.rcrd_create_user_id;
	r_new_hstry.rcrd_create_ts				:= NEW.rcrd_create_ts;
	r_new_hstry.rcrd_create_ip				:= NEW.rcrd_create_ip;
		
	r_new_hstry.hst_rcrd_create_user_id 	:= sbs_util.get_audit_user();
	r_new_hstry.hst_rcrd_create_ip      	:= inet_client_addr();
	r_new_hstry.hst_rcrd_create_ts      	:= CURRENT_TIMESTAMP;
		
	IF TG_OP = 'DELETE' THEN
		r_old_hstry.rcrd_dml_type_cd		:= 'D-OLD';

		INSERT INTO pub_admin.pba_event_name_hst_t (EVENT_NAME_ID, EVENT_CONSTANT, EVENT_NAME, EVENT_NAME_DESC, RCRD_CREATE_USER_ID, RCRD_CREATE_IP, RCRD_CREATE_TS, 
		                                         RCRD_DML_TYPE_CD, rcrd_updt_user_id, rcrd_updt_ip, rcrd_updt_ts, HST_RCRD_CREATE_USER_ID, HST_RCRD_CREATE_IP,
												 HST_RCRD_CREATE_TS)
		VALUES (r_old_hstry.event_name_id, r_old_hstry.event_constant, r_old_hstry.event_name, r_old_hstry.event_name_desc, r_old_hstry.RCRD_CREATE_USER_ID, 
		       r_old_hstry.RCRD_CREATE_IP, r_old_hstry.rcrd_create_ts, r_old_hstry.rcrd_dml_type_cd, r_old_hstry.rcrd_updt_user_id, r_old_hstry.rcrd_updt_ip,
			   r_old_hstry.rcrd_updt_ts, r_old_hstry.hst_rcrd_create_user_id, r_old_hstry.hst_rcrd_create_ip, r_old_hstry.hst_rcrd_create_ts);

	ELSIF (TG_OP = 'UPDATE') THEN
		r_old_hstry.rcrd_dml_type_cd		:= 'U-OLD';

		INSERT INTO pub_admin.pba_event_name_hst_t (EVENT_NAME_ID, EVENT_CONSTANT, EVENT_NAME, EVENT_NAME_DESC, RCRD_CREATE_USER_ID, RCRD_CREATE_IP, RCRD_CREATE_TS, 
		                                        RCRD_DML_TYPE_CD, rcrd_updt_user_id, rcrd_updt_ip, rcrd_updt_ts, HST_RCRD_CREATE_USER_ID, HST_RCRD_CREATE_IP,
												HST_RCRD_CREATE_TS)
		VALUES (r_old_hstry.event_name_id, r_old_hstry.event_constant, r_old_hstry.event_name, r_old_hstry.event_name_desc, r_old_hstry.RCRD_CREATE_USER_ID, 
		       r_old_hstry.RCRD_CREATE_IP, r_old_hstry.rcrd_create_ts, r_old_hstry.rcrd_dml_type_cd, r_old_hstry.rcrd_updt_user_id, r_old_hstry.rcrd_updt_ip,
			   r_old_hstry.rcrd_updt_ts, r_old_hstry.hst_rcrd_create_user_id, r_old_hstry.hst_rcrd_create_ip, r_old_hstry.hst_rcrd_create_ts);

		r_new_hstry.rcrd_dml_type_cd		:= 'U-NEW';

		INSERT INTO pub_admin.pba_event_name_hst_t (EVENT_NAME_ID, EVENT_CONSTANT, EVENT_NAME, EVENT_NAME_DESC, RCRD_CREATE_USER_ID, RCRD_CREATE_IP, RCRD_CREATE_TS, 
		                                        RCRD_DML_TYPE_CD, rcrd_updt_user_id, rcrd_updt_ip, rcrd_updt_ts, HST_RCRD_CREATE_USER_ID, HST_RCRD_CREATE_IP,
												HST_RCRD_CREATE_TS)
		VALUES (r_new_hstry.event_name_id, r_new_hstry.event_constant, r_new_hstry.event_name, r_new_hstry.event_name_desc, r_new_hstry.RCRD_CREATE_USER_ID, 
		       r_new_hstry.rcrd_create_ip, r_new_hstry.rcrd_create_ts, r_new_hstry.rcrd_dml_type_cd, r_new_hstry.rcrd_updt_user_id, r_new_hstry.rcrd_updt_ip, 
			   r_new_hstry.rcrd_updt_ts, r_new_hstry.hst_rcrd_create_user_id, r_new_hstry.hst_rcrd_create_ip, r_new_hstry.hst_rcrd_create_ts);
	ELSIF (TG_OP = 'INSERT') THEN
	 	r_new_hstry.rcrd_dml_type_cd		:= 'I-NEW';

		INSERT INTO pub_admin.pba_event_name_hst_t (EVENT_NAME_ID, EVENT_CONSTANT, EVENT_NAME, EVENT_NAME_DESC, RCRD_CREATE_USER_ID, RCRD_CREATE_IP, RCRD_CREATE_TS, 
		                                         RCRD_DML_TYPE_CD, rcrd_updt_user_id, rcrd_updt_ip, rcrd_updt_ts, HST_RCRD_CREATE_USER_ID, HST_RCRD_CREATE_IP,
												 HST_RCRD_CREATE_TS)
		VALUES (r_new_hstry.event_name_id, r_new_hstry.event_constant, r_new_hstry.event_name, r_new_hstry.event_name_desc, r_new_hstry.RCRD_CREATE_USER_ID, 
		       r_new_hstry.rcrd_create_ip, r_new_hstry.rcrd_create_ts, r_new_hstry.rcrd_dml_type_cd, r_new_hstry.rcrd_updt_user_id, r_new_hstry.rcrd_updt_ip, 
			   r_new_hstry.rcrd_updt_ts, r_new_hstry.hst_rcrd_create_user_id, r_new_hstry.hst_rcrd_create_ip, r_new_hstry.hst_rcrd_create_ts);
	END IF;
	
	RETURN NEW;
EXCEPTION
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in Procedure :- PBA_TRG_FNC_EVENT_NAME_ARIUD(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack 		=> TRUE
			);
END;
$$
LANGUAGE plpgsql VOLATILE;
