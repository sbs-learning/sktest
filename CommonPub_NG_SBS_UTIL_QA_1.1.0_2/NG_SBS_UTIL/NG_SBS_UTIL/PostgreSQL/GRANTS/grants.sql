grant execute on procedure sbs_util.error_handler(
	in_error_stack character varying,
	in_error_code character varying,
	in_error_message character varying,
	in_show_stack boolean) to public;
grant execute on procedure sbs_util.error_handler(
	in_error_code character varying,
	in_error_message character varying) to public;
grant execute on procedure sbs_util.p_util_get_all_versions  to public;
grant execute on procedure sbs_util.p_util_enforce_version  to public;
grant execute on procedure sbs_util.p_util_log_apply  to public;
grant execute on procedure sbs_util.p_util_log_validation  to public;
grant execute on procedure sbs_util.refresh_mv  to public;
grant execute on procedure sbs_util.set_sequence_from_column  to public;
grant execute on procedure sbs_util.set_sequence_nextval  to public;
grant execute on procedure sbs_util.grant_to_pub_work_user  to public;
grant execute on procedure sbs_util.grant_to_raw  to public;
grant execute on procedure sbs_util.grant_to_ssctrg  to public;
grant execute on procedure sbs_util.grant_to_pub_admin_user  to public;
grant execute on procedure sbs_util.grant_to_pub_work  to public;
grant execute on procedure sbs_util.grant_to_ssctrg  to public;
grant execute on procedure sbs_util.grant_to_pub_admin  to public;
grant execute on procedure sbs_util.grant_to_bl_user to public;
grant execute on procedure sbs_util.grant_to_bl to public;
grant execute on procedure sbs_util.grant_to_ctrg to public;
grant execute on procedure sbs_util.grant_to_lt to public;
grant execute on procedure sbs_util.grant_to_ctrg_support to public;
grant execute on procedure sbs_util.grant_to_trg to public;
grant execute on procedure sbs_util.gather_table_stats to public;
grant execute on procedure sbs_util.gather_table_partition_stats to public;
grant execute on procedure sbs_util.drop_partition to public;
grant execute on procedure sbs_util.drop_column to public;
grant execute on procedure sbs_util.p_util_sbs_set_view to public;
-- Relese 1.1.0
grant execute on procedure sbs_util.request_lock to public;
grant execute on procedure sbs_util.p_util_request_lock to public;
grant execute on procedure sbs_util.release_lock to public;
grant execute on procedure sbs_util.p_util_release_lock to public;

grant execute on function sbs_util.p_util_get_version to public;
grant execute on function sbs_util.p_util_get_numeric_version to public;
grant execute on function sbs_util.get_version_prefix to public;
grant execute on function sbs_util.p_util_last_applied_version_fnc to public;
grant execute on function sbs_util.get_audit_user  to public;
grant execute on function sbs_util.get_base  to public;
grant execute on function sbs_util.get_base36  to public;
grant execute on function sbs_util.get_bl_schema  to public;
grant execute on function sbs_util.get_bl_user_schema  to public;
grant execute on function sbs_util.get_ctrg_schema  to public;
grant execute on function sbs_util.get_hash_text  to public;
grant execute on function sbs_util.get_hash_varchar  to public;
grant execute on function sbs_util.get_pub_admin_user_schema  to public;
grant execute on function sbs_util.get_pub_work_schema  to public;
grant execute on function sbs_util.get_pub_work_user_schema  to public;
grant execute on function sbs_util.get_pub_admin_schema  to public;
grant execute on function sbs_util.get_raw_schema  to public;
grant execute on function sbs_util.get_ssctrg_schema  to public;
grant execute on function sbs_util.get_ctrg_support_schema  to public;
grant execute on function sbs_util.get_lt_schema  to public;
grant execute on function sbs_util.get_trg_schema  to public;
grant execute on function sbs_util.p_util_sbs_get_view to public;

-- Relese 1.1.0
grant execute on function sbs_util.get_sql_for_grant to public;
grant execute on function sbs_util.sut_trg_fnc_sbs_view_script_ariud to public;
grant execute on function sbs_util.sut_trg_fnc_sbs_view_script_briu to public;
grant execute on function sbs_util.allocate_unique_lockhandle to public;

grant select on table sbs_util.sbs_util_version to public;
-- Relese 1.1.0
grant select,insert,update,delete on table sbs_util.sbs_view_script to public;
grant select,insert,update,delete on table sbs_util.sbs_view_script_hst_t to public;
