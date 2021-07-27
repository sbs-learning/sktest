CREATE OR REPLACE FUNCTION sbs_util.SUT_TRG_FNC_SBS_VIEW_SCRIPT_ARIUD()
  RETURNS TRIGGER
  AS
$$
DECLARE
	r_old_hstry sbs_util.sbs_view_script_hst_t%ROWTYPE;
	r_new_hstry sbs_util.sbs_view_script_hst_t%ROWTYPE;
	l_format_call_stack TEXT;
BEGIN

	r_old_hstry.vw_id						:= OLD.vw_id;
	r_old_hstry.fq_vw_name					:= OLD.fq_vw_name;
	r_old_hstry.vw_script					:= OLD.vw_script;
	
	r_old_hstry.rcrd_create_user_id			:= OLD.rcrd_create_user_id;
	r_old_hstry.rcrd_create_ts				:= OLD.rcrd_create_ts;
	r_old_hstry.rcrd_create_ip				:= OLD.rcrd_create_ip;

	r_old_hstry.rcrd_updt_user_id           := OLD.rcrd_updt_user_id;
	r_old_hstry.rcrd_updt_ip                := OLD.rcrd_updt_ip;
	r_old_hstry.rcrd_updt_ts                := OLD.rcrd_updt_ts;

	r_old_hstry.hst_rcrd_create_user_id 	:= sbs_util.get_audit_user();
	r_old_hstry.hst_rcrd_create_ip      	:= inet_client_addr();
	r_old_hstry.hst_rcrd_create_ts      	:= CURRENT_TIMESTAMP;

----------------------------------------------------------------------------------------
	r_new_hstry.vw_id						:= NEW.vw_id;
	r_new_hstry.fq_vw_name					:= NEW.fq_vw_name;
	r_new_hstry.vw_script					:= NEW.vw_script;

	r_new_hstry.rcrd_create_user_id			:= NEW.rcrd_create_user_id;
	r_new_hstry.rcrd_create_ts				:= NEW.rcrd_create_ts;
	r_new_hstry.rcrd_create_ip				:= NEW.rcrd_create_ip;

	r_new_hstry.rcrd_updt_user_id           := NEW.rcrd_updt_user_id;
	r_new_hstry.rcrd_updt_ip                := NEW.rcrd_updt_ip;
	r_new_hstry.rcrd_updt_ts                := NEW.rcrd_updt_ts;
			
	r_new_hstry.hst_rcrd_create_user_id 	:= sbs_util.get_audit_user();
	r_new_hstry.hst_rcrd_create_ip      	:= inet_client_addr();
	r_new_hstry.hst_rcrd_create_ts      	:= CURRENT_TIMESTAMP;
		
	IF TG_OP = 'DELETE' THEN
		r_old_hstry.rcrd_dml_type_cd		:= 'D-OLD';

		INSERT INTO sbs_util.sbs_view_script_hst_t (VW_ID, FQ_VW_NAME, VW_SCRIPT, RCRD_CREATE_USER_ID, RCRD_CREATE_IP, RCRD_CREATE_TS, 
		                                         RCRD_DML_TYPE_CD, RCRD_UPDT_USER_ID, RCRD_UPDT_IP, RCRD_UPDT_TS, HST_RCRD_CREATE_USER_ID, HST_RCRD_CREATE_IP,
												 HST_RCRD_CREATE_TS)
		VALUES (r_old_hstry.vw_id, r_old_hstry.fq_vw_name, r_old_hstry.vw_script, r_old_hstry.rcrd_create_user_id, 
		       r_old_hstry.rcrd_create_ip, r_old_hstry.rcrd_create_ts, r_old_hstry.rcrd_dml_type_cd, r_old_hstry.rcrd_updt_user_id, r_old_hstry.rcrd_updt_ip,
			   r_old_hstry.rcrd_updt_ts, r_old_hstry.hst_rcrd_create_user_id, r_old_hstry.hst_rcrd_create_ip, r_old_hstry.hst_rcrd_create_ts);

	ELSIF (TG_OP = 'UPDATE') THEN
		r_old_hstry.rcrd_dml_type_cd		:= 'U-OLD';

		INSERT INTO sbs_util.sbs_view_script_hst_t (VW_ID, FQ_VW_NAME, VW_SCRIPT, RCRD_CREATE_USER_ID, RCRD_CREATE_IP, RCRD_CREATE_TS, 
		                                        RCRD_DML_TYPE_CD, RCRD_UPDT_USER_ID, RCRD_UPDT_IP, RCRD_UPDT_TS, HST_RCRD_CREATE_USER_ID, HST_RCRD_CREATE_IP,
												HST_RCRD_CREATE_TS)
		VALUES (r_old_hstry.vw_id, r_old_hstry.fq_vw_name, r_old_hstry.vw_script, r_old_hstry.rcrd_create_user_id, 
		       r_old_hstry.RCRD_CREATE_IP, r_old_hstry.rcrd_create_ts, r_old_hstry.rcrd_dml_type_cd, r_old_hstry.rcrd_updt_user_id, r_old_hstry.rcrd_updt_ip,
			   r_old_hstry.rcrd_updt_ts, r_old_hstry.hst_rcrd_create_user_id, r_old_hstry.hst_rcrd_create_ip, r_old_hstry.hst_rcrd_create_ts);

		r_new_hstry.rcrd_dml_type_cd		:= 'U-NEW';

		INSERT INTO sbs_util.sbs_view_script_hst_t (VW_ID, FQ_VW_NAME, VW_SCRIPT, RCRD_CREATE_USER_ID, RCRD_CREATE_IP, RCRD_CREATE_TS, 
		                                        RCRD_DML_TYPE_CD, RCRD_UPDT_USER_ID, RCRD_UPDT_IP, RCRD_UPDT_TS, HST_RCRD_CREATE_USER_ID, HST_RCRD_CREATE_IP,
												HST_RCRD_CREATE_TS)
		VALUES (r_new_hstry.vw_id, r_new_hstry.fq_vw_name, r_new_hstry.vw_script, r_new_hstry.rcrd_create_user_id,
		       r_new_hstry.rcrd_create_ip, r_new_hstry.rcrd_create_ts, r_new_hstry.rcrd_dml_type_cd, r_new_hstry.rcrd_updt_user_id, r_new_hstry.rcrd_updt_ip, 
			   r_new_hstry.rcrd_updt_ts, r_new_hstry.hst_rcrd_create_user_id, r_new_hstry.hst_rcrd_create_ip, r_new_hstry.hst_rcrd_create_ts);
	ELSIF (TG_OP = 'INSERT') THEN
	 	r_new_hstry.rcrd_dml_type_cd		:= 'I-NEW';

		INSERT INTO sbs_util.sbs_view_script_hst_t (VW_ID, FQ_VW_NAME, VW_SCRIPT, RCRD_CREATE_USER_ID, RCRD_CREATE_IP, RCRD_CREATE_TS, 
		                                         RCRD_DML_TYPE_CD, RCRD_UPDT_USER_ID, RCRD_UPDT_IP, RCRD_UPDT_TS, HST_RCRD_CREATE_USER_ID, HST_RCRD_CREATE_IP,
												 HST_RCRD_CREATE_TS)
		VALUES (r_new_hstry.vw_id, r_new_hstry.fq_vw_name, r_new_hstry.vw_script, r_new_hstry.rcrd_create_user_id, 
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
				in_error_message 	=> CONCAT(' Error in Procedure :- SUT_TRG_FNC_SBS_VIEW_SCRIPT_ARIUD(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack 		=> TRUE
			);
END;
$$
LANGUAGE plpgsql VOLATILE;
