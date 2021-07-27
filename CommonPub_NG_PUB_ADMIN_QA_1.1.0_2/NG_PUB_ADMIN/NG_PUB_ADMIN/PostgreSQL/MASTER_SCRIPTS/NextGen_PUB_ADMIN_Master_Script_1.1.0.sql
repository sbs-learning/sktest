--**************************************************************************************************
--*                                                                                                *
--*        NextGen PUB ADMIN 1.1.0 Install Script (from empty schema)                              *
--*                                                                                                *
--*        NextGen PUB ADMIN 1.1.0 empty schema with control data.                                 *
--**************************************************************************************************
--* Revision History                                                                               *
--* YYYYMMDD Revisor 		Build 		Comment                                                    *
--* -------- ------- ----- ----------------------------------------------------------------------- *
--* 20210629 Akshay           1         Created deployment script                                  *
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


---------DDL
\ir  '../DDL/PBA_VERSION.sql'

\ir  '../DDL/PBA_PARAM_T.sql'

\ir  '../DDL/PBA_EVENT_NAME_T.sql'

\ir  '../DDL/PBA_CNSTRNT_T.sql'

\ir  '../DDL/PBA_EVENT_T.sql'

\ir  '../DDL/PBA_PARAM_HST_T.sql'

\ir  '../DDL/PBA_EVENT_NAME_HST_T.sql'
--**********************************

---------Functions
\ir  '../FUNCTIONS/FNC_CONNECT_TO_DB.sql'

\ir  '../FUNCTIONS/FNC_GET_DB_LINK_NAME.sql'

\ir  '../FUNCTIONS/GET_PARAMETER.sql'

\ir  '../FUNCTIONS/GET_PARTITION_NAME.sql'

\ir  '../FUNCTIONS/GET_PARTITION_VALUE.sql'

\ir  '../FUNCTIONS/P_UTIL_GET_PARTITION_NAME.sql'

\ir  '../FUNCTIONS/P_UTIL_GET_PARTITION_VALUE.sql'

--Release 1.1.0
\ir  '../FUNCTIONS/P_UTIL_VALIDATE_PARTITION.sql'


--**********************************

---------Triggers Functions

\ir  '../FUNCTIONS/PBA_TRG_FNC_PARAM_BRIU.sql'

\ir  '../FUNCTIONS/PBA_TRG_FNC_PARAM_ARIUD.sql'

\ir  '../FUNCTIONS/PBA_TRG_FNC_EVENT_NAME_BRIU.sql'

\ir  '../FUNCTIONS/PBA_TRG_FNC_EVENT_NAME_ARIUD.sql'

--**********************************

---------Triggers

\ir  '../TRIGGERS/PBA_PARAM_BRIU.sql'

\ir  '../TRIGGERS/PBA_PARAM_ARIUD.sql'

\ir  '../TRIGGERS/PBA_EVENT_NAME_BRIU.sql'

\ir  '../TRIGGERS/PBA_EVENT_NAME_ARIUD.sql'

--**********************************

---------Procedures
\ir  '../PROCEDURES/DELETE_CONSTRAINTS.sql'

\ir  '../PROCEDURES/DROP_PARTITION.sql'

\ir  '../PROCEDURES/GET_CONSTRAINTS.sql'

\ir  '../PROCEDURES/HANDLE_CONSTRAINTS.sql'

\ir  '../PROCEDURES/HANDLE_TABLE_N_PARTITION_STATS.sql'

\ir  '../PROCEDURES/HANDLE_TABLESTATS.sql'

\ir  '../PROCEDURES/P_CTRL_LOG_EVENT.sql'

\ir  '../PROCEDURES/P_UTIL_DROP_PARTITION.sql'

\ir  '../PROCEDURES/P_UTIL_GATHER_FQ_TABLE_N_PARTITION_STATS.sql'

\ir  '../PROCEDURES/P_UTIL_GATHER_FQ_TABLE_STATS.sql'

\ir  '../PROCEDURES/P_UTIL_GATHER_TABLE_N_PARTITION_STATS.sql'

\ir  '../PROCEDURES/P_UTIL_GATHER_TABLE_STATS.sql'

\ir  '../PROCEDURES/P_UTIL_REFRESH_MV.sql' 

\ir  '../PROCEDURES/PRC_LOG_EVENT.sql'

\ir  '../PROCEDURES/REFRESH_MV.sql'

\ir  '../PROCEDURES/SAVE_CONSTRAINTS.sql'

\ir  '../PROCEDURES/SET_PARAMETER.sql'

--Release 1.1.0
\ir  '../PROCEDURES/ADD_RANGE_PARTITION.sql'

\ir  '../PROCEDURES/P_UTIL_ADD_RANGE_PARTITION.sql'

--**********************************

---------DML
\ir  '../DML/PBA_EVENT_NAME_T.sql'

\ir  '../DML/PBA_PARAM_T.sql'

--**********************************

--------- Update Permissions of Pub Admin objects
\ir '../GRANTS/GRANTS.sql'
--**********************************

--------- Update version in PBA_VERSION table
call sbs_util.p_util_log_apply('INITIAL','1.1.0','NG_PUB_ADMIN','Release 1.1.0, installed from initial script ','pba_version',NULL);
