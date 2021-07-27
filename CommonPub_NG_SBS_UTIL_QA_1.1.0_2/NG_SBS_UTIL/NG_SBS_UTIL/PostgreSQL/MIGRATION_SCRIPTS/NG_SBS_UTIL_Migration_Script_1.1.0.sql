--**************************************************************************************************
--*                                                                                                *
--*			NextGen SBS UTIL 1.1.0 Install Script (from empty schema)                          *
--*                                                                                                *
--*			NextGen SBS UTIL 1.1.0 empty schema with control data.                                  *
--**************************************************************************************************
--* Revision History                                                                               *
--* YYYYMMDD Revisor 		Build 		Comment                                                    *
--* -------- ------- ----- ----------------------------------------------------------------------- *
--* 20210426 Sakshi Jain      1         Created deployment script                                  *
--* 20210630 Kalyan Kumar     1         Created deployment script                                  *
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


call sbs_util.enforce_version(in_from_version => '1.0.0', in_to_version => NULL, in_version_table => 'sbs_util_version');

call sbs_util.log_validation('1.1.0', 'NG_SBS_UTIL', 'Release 1.1.0; upgraded from 1.0.0', 'sbs_util_version', NULL);

---------DDL

-- Relese 1.1.0
\ir  '../DDL/sbs_view_script.sql'

\ir  '../DDL/sbs_view_script_hst_t.sql'

\ir  '../DDL/sbs_pg_session_lock.sql'

--**********************************

---------Functions
-- Relese 1.1.0
\ir  '../FUNCTIONS/get_hash_text.sql'

\ir  '../FUNCTIONS/get_hash_varchar.sql'

\ir  '../FUNCTIONS/get_sql_for_grant.sql'

\ir  '../FUNCTIONS/p_util_sbs_get_view.sql'

\ir  '../FUNCTIONS/sut_trg_fnc_sbs_view_script_ariud.sql'

\ir  '../FUNCTIONS/sut_trg_fnc_sbs_view_script_briu.sql'

\ir  '../FUNCTIONS/allocate_unique_lockhandle.sql'


--**********************************

---------Procedures

-- Relese 1.1.0

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

\ir  '../PROCEDURES/p_util_sbs_set_view.sql'

\ir  '../PROCEDURES/request_lock.sql'

\ir  '../PROCEDURES/p_util_request_lock.sql'

\ir  '../PROCEDURES/release_lock.sql'

\ir  '../PROCEDURES/p_util_release_lock.sql'

--**********************************

---------TRIGGERS
-- Relese 1.1.0
\ir  '../TRIGGERS/sut_sbs_view_script_ariud.sql'

\ir  '../TRIGGERS/sut_sbs_view_script_briu.sql'


--------- Update Permissions of SBS UTIL objects to PUBLIC
\ir '../GRANTS/grants.sql'
--**********************************

--------- Update version in SBS_UTIL_VERSION table
call sbs_util.p_util_log_apply('UPGRADE','1.1.0','NG_SBS_UTIL','Release 1.1.0, upgraded from 1.0.0','sbs_util_version',NULL);