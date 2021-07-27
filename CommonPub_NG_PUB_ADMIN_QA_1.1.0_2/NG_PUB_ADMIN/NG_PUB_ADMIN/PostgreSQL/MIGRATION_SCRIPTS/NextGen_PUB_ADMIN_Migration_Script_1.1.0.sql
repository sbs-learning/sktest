--**************************************************************************************************
--*        NextGen-Migration Script from 1.0.0 to 1.1.0                                            *
--*        NextGen PUB ADMIN 1.1.0 Install Script (from empty schema)                              *
--*                                                                                                *
--*        This script performs the actual upgrade from 1.0.0 to 1.1.0.                            *
--*        NextGen PUB ADMIN 1.1.0 empty schema with control data.                                 *
--**************************************************************************************************
--* Revision History                                                                               *
--* YYYYMMDD Revisor 		Build 		Comment                                                    *
--* -------- ------- ----- ----------------------------------------------------------------------- *
--* 20210629 Akshay           1         Created Migration script                                  *
--*                                                                                                *
--**************************************************************************************************                                                                           *
--**************************************************************************************************

\conninfo
--****************
\pset format unaligned
--*****************
\timing on
--**********

\set ON_ERROR_STOP off

show search_path;
set client_encoding to 'UTF8';
set search_path = pub_admin;
show search_path;

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

call sbs_util.enforce_version(in_from_version => '1.0.0', in_to_version => NULL, in_version_table => 'pba_version');

call sbs_util.log_validation('1.1.0', 'NG_PUB_ADMIN', 'Release 1.1.0; upgraded from 1.0.0', 'pba_version', NULL);

---------DML
\ir  '../DML/PBA_EVENT_NAME_T.sql'

---------Functions

\ir  '../FUNCTIONS/P_UTIL_VALIDATE_PARTITION.sql'


--**********************************

---------Procedures

\ir  '../PROCEDURES/P_CTRL_LOG_EVENT.sql'

\ir  '../PROCEDURES/ADD_RANGE_PARTITION.sql'

\ir  '../PROCEDURES/P_UTIL_ADD_RANGE_PARTITION.sql'

--**********************************

--------- Update Permissions of Pub Admin objects
\ir '../GRANTS/GRANTS.sql'
--**********************************

--------- Update version in PBA_VERSION table
call sbs_util.p_util_log_apply('UPGRADE','1.1.0','NG_PUB_ADMIN','Release 1.1.0; upgraded from 1.0.0 ','pba_version',NULL);
