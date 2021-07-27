--**************************************************************************************************
--*                                                                                                *
--*             NextGen SBS UTIL 1.0.0 Install Script (from empty schema)                          *
--*                                                                                                *
--*        NextGen SBS UTIL 1.0.0 empty schema with control data.                                  *
--**************************************************************************************************
--* Revision History                                                                               *
--* YYYYMMDD Revisor 		Build 		Comment                                                    *
--* -------- ------- ----- ----------------------------------------------------------------------- *
--* 20210426 Sakshi Jain      1         Created deployment script                                  *
--*                                                                                                *
--**************************************************************************************************                                                                           *
--**************************************************************************************************
show search_path;

set client_encoding to 'UTF8';
set search_path = sbs_util;
show search_path;

\conninfo
--****************
\pset format unaligned
--*****************
\timing on
--**********

\set ON_ERROR_STOP off

-- Set up prompt with user@database
-- Define local PSQL variables to capture results of WHOAMI query
WITH whoami AS
 (SELECT current_schema as schemaname,current_user as username
        ,now() as ctime
        ,current_database() as db_name)
SELECT w.schemaname || '@' || w.db_name AS gname, w.username
      ,w.schemaname
      ,w.ctime
      ,w.db_name sname
  FROM whoami w;
\gset
\set PROMPT1 '%:gname:> '

---------DDL
\ir  '../DDL/sbs_util_version.sql'
--**********************************

\ir  '../PROCEDURES/error_handler_2_param.sql'

\ir  '../PROCEDURES/error_handler_4_param.sql'

---------Functions
\ir  '../FUNCTIONS/get_audit_user.sql'

\ir  '../FUNCTIONS/get_base.sql'

\ir  '../FUNCTIONS/get_base36.sql'

\ir  '../FUNCTIONS/get_bl_schema.sql'

\ir  '../FUNCTIONS/get_bl_user_schema.sql'

\ir  '../FUNCTIONS/get_ctrg_schema.sql'

\ir  '../FUNCTIONS/get_pub_admin_user_schema.sql'

\ir  '../FUNCTIONS/get_pub_work_schema.sql'

\ir  '../FUNCTIONS/get_pub_work_user_schema.sql'

\ir  '../FUNCTIONS/get_pub_admin_schema.sql'

\ir  '../FUNCTIONS/get_raw_schema.sql'

\ir  '../FUNCTIONS/get_ctrg_support_schema.sql'

\ir  '../FUNCTIONS/get_ssctrg_schema.sql'

\ir  '../FUNCTIONS/get_lt_schema.sql'

\ir  '../FUNCTIONS/get_trg_schema.sql'

\ir  '../FUNCTIONS/get_schema_owner.sql'

\ir  '../FUNCTIONS/are_objects_stats_stale.sql'

\ir  '../FUNCTIONS/get_hash_text.sql'

\ir  '../FUNCTIONS/get_hash_varchar.sql'

\ir  '../FUNCTIONS/get_numeric_version.sql'

\ir  '../FUNCTIONS/get_version.sql'

\ir  '../FUNCTIONS/get_version_prefix.sql'

\ir  '../FUNCTIONS/last_applied_version_fnc.sql'

\ir  '../FUNCTIONS/p_util_get_version.sql'

\ir  '../FUNCTIONS/p_util_get_numeric_version.sql'

\ir  '../FUNCTIONS/p_util_last_applied_version_fnc.sql'
--**********************************

---------Procedures
\ir  '../PROCEDURES/refresh_mv.sql'

\ir  '../PROCEDURES/set_sequence_nextval.sql'

\ir  '../PROCEDURES/set_sequence_from_column.sql'

\ir  '../PROCEDURES/grant_to_pub_work_user.sql'

\ir  '../PROCEDURES/grant_to_raw.sql'

\ir  '../PROCEDURES/grant_to_ssctrg.sql'

\ir  '../PROCEDURES/grant_to_pub_admin_user.sql'

\ir  '../PROCEDURES/grant_to_ctrg_support.sql'

\ir  '../PROCEDURES/grant_to_pub_admin.sql'

\ir  '../PROCEDURES/grant_to_pub_work.sql'

\ir  '../PROCEDURES/grant_to_bl_user.sql'

\ir  '../PROCEDURES/grant_to_bl.sql'

\ir  '../PROCEDURES/grant_to_ctrg.sql'

\ir  '../PROCEDURES/grant_to_trg.sql'

\ir  '../PROCEDURES/grant_to_lt.sql'

\ir  '../PROCEDURES/gather_table_stats.sql' 

\ir  '../PROCEDURES/gather_table_partition_stats.sql'

\ir  '../PROCEDURES/drop_partition.sql'

\ir  '../PROCEDURES/drop_column.sql'

\ir  '../PROCEDURES/enforce_version.sql'

\ir  '../PROCEDURES/log_validation.sql'

\ir  '../PROCEDURES/log_apply.sql'

\ir  '../PROCEDURES/get_all_versions.sql'

\ir  '../PROCEDURES/p_util_get_all_versions.sql'

\ir  '../PROCEDURES/p_util_enforce_version.sql'

\ir  '../PROCEDURES/p_util_log_validation.sql'

\ir  '../PROCEDURES/p_util_log_apply.sql'
--**********************************

--------- Update Permissions of SBS UTIL objects to PUBLIC
\ir '../GRANTS/grants.sql'
--**********************************

--------- Update version in SBS_UTIL_VERSION table
call sbs_util.p_util_log_apply('INITIAL','1.0.0','NG_SBS_UTIL','Release 1.0.0, installed from initial script ','sbs_util_version',NULL);