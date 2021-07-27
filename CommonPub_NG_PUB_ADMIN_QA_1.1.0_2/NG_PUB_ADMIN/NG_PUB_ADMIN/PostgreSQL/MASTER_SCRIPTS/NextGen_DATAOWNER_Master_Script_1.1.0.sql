--**************************************************************************************************
--*                                                                                                *
--*        NextGen PUB ADMIN 1.1.0 Install Script (from empty schema)                              *
--*                                                                                                *
--*        NextGen PUB ADMIN 1.1.0 empty schema with control data.                                 *
--**************************************************************************************************
--* Revision History                                                                               *
--* YYYYMMDD Revisor 		Build 		Comment                                                    *
--* -------- ------- ----- ----------------------------------------------------------------------- *
--* 20210701 Akshay            1         Created deployment script                                 *
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

show search_path;
set client_encoding to 'UTF8';
set search_path = :schemaname;
show search_path;


\ir  '../FUNCTIONS/P_UTIL_GET_PARTITION_NAME.sql'

\ir  '../FUNCTIONS/P_UTIL_GET_PARTITION_VALUE.sql'

\ir  '../FUNCTIONS/P_UTIL_VALIDATE_PARTITION.sql'

\ir  '../PROCEDURES/P_UTIL_DROP_PARTITION.sql'

\ir  '../PROCEDURES/P_UTIL_GATHER_FQ_TABLE_N_PARTITION_STATS.sql'

\ir  '../PROCEDURES/P_UTIL_GATHER_FQ_TABLE_STATS.sql'

\ir  '../PROCEDURES/P_UTIL_GATHER_TABLE_N_PARTITION_STATS.sql'

\ir  '../PROCEDURES/P_UTIL_GATHER_TABLE_STATS.sql'

\ir  '../PROCEDURES/P_UTIL_REFRESH_MV.sql' 

\ir  '../PROCEDURES/P_UTIL_ADD_RANGE_PARTITION.sql'