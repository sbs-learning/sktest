CREATE OR REPLACE PROCEDURE sbs_util.log_apply
(
	in_apply_method		CHARACTER VARYING,
	in_version			CHARACTER VARYING,
	in_description		CHARACTER VARYING,
	in_comments			CHARACTER VARYING,
	in_version_table	CHARACTER VARYING,
	in_version_seq		CHARACTER VARYING DEFAULT NULL
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
      -------------------------------------------------------------------------------------
      -- Name : log_apply
      -------------------------------------------------------------------------------------
      -- Description : Logs the validation (pre-check) for a version in version table
      --
      -- Logic : Verify the version is in a suitable state, and either insert or update
      --         into version table
      --
      --Input Parameter details
      --IN_APPLY_METHOD   - It can be INITIAL or UPGRADE
      --IN_VERSION        - Version to which the tool will be upgraded
      --IN_DESCRIPTION    - Description
      --IN_COMMENTS        - Comment about the release
      --IN_VERSION_TABLE  - Version table of the tool
      --IN_VERSION_SEQ    - Sequence on Version table of the tool
	  ------------------------------------------------------------------------------------------------
      -- RefNo Name            JIRA NO 		Date         Description of change            
      -- ----- ---------------- -------- -------------------------------------------------------------
      -- 	1	Akshay	     CSPUBCC-4301	04/08/2021	log_apply() initial draft
      -- 	2	Akshay	     CSPUBCC-4383	04/15/2021	Added variable l_apply_method, l_version, l_description,
      --                                                l_comments, l_version_table, l_version_seq and upper/lower function in query.
      -------------------------------------------------------------------------------------------------
      l_status                VARCHAR(30);
      L_VERSION_COUNT         numeric;
      l_sql_txt               varchar(32767);
	  co_single_quote         VARCHAR(1) DEFAULT CHR(39);
	  gc_apply_method_upgrade VARCHAR(20) DEFAULT 'UPGRADE';
	  gc_status_applied       VARCHAR(20) DEFAULT 'APPLIED';
	  gc_apply_method_initial VARCHAR(20) DEFAULT 'INITIAL';
	  l_apply_method          VARCHAR(10);
	  l_version               VARCHAR(50);
	  l_description           VARCHAR(100);
	  l_comments              VARCHAR(500);
	  l_version_table         VARCHAR(100);
	  l_version_seq           VARCHAR(100);
	  l_format_call_stack     Text;
	  
BEGIN
      l_apply_method  := UPPER(TRIM(IN_APPLY_METHOD));
      l_version       := TRIM(IN_VERSION);
      l_description   := TRIM(IN_DESCRIPTION);
      l_comments	  := TRIM(IN_COMMENTS);
	  l_version_table := LOWER(TRIM(IN_VERSION_TABLE));
	  l_version_seq   := LOWER(TRIM(IN_VERSION_SEQ));

      --if the users wants to upgrade from a version
    IF l_apply_method = gc_apply_method_upgrade THEN
        CALL sbs_util.enforce_version (sbs_util.last_applied_version_fnc (l_version_table), l_version,l_version_table);

         --Unset the last installed version as current version
        L_SQL_TXT:='UPDATE '||l_version_table||' SET IS_CURRENT=NULL WHERE IS_CURRENT=1';
        EXECUTE L_SQL_TXT;
         --if enforce_version is successful change the STATUS to APPLIED
        L_SQL_TXT:='UPDATE '||l_version_table||
                   ' SET description = '||co_single_quote||l_description||co_single_quote||',
                   comments = '||co_single_quote||l_comments||co_single_quote||',
                   status = '||co_single_quote||gc_status_applied||co_single_quote||',
                   APPLIED_DATE = current_timestamp,
				   APPLIED_BY = '||co_single_quote||sbs_util.get_audit_user ()||co_single_quote||',
                   IS_CURRENT = 1 WHERE version = '||co_single_quote||l_version||co_single_quote;
        EXECUTE L_SQL_TXT;
    ELSE
    --if the tool is installed for the first time , i.e. INITIAL INSTALL
        IF l_apply_method = gc_apply_method_initial THEN
         --if there is no previous version installed of the tool
            IF sbs_util.last_applied_version_fnc (l_version_table) IS NULL THEN
				IF(l_version_seq is  NOT NULL) THEN
						L_SQL_TXT:='INSERT INTO '||l_version_table||' (VERSION_ID,
																		DESCRIPTION,
																		VERSION,
																		COMMENTS,
																		APPLIED_DATE,
																		STATUS,
																		VERIFICATION_DATE,
																		VERIFIED_BY_USERNAME,
																		APPLY_METHOD,
																		APPLIED_BY,
																		IS_CURRENT)
									VALUES (NEXTVAL('''||l_version_seq||''')'||','
											 ||co_single_quote||l_description||co_single_quote||','
											 ||co_single_quote||l_version||co_single_quote||','
											 ||CO_SINGLE_QUOTE||l_comments||CO_SINGLE_QUOTE||','
											 ||CO_SINGLE_QUOTE||current_timestamp||CO_SINGLE_QUOTE||','
											 ||CO_SINGLE_QUOTE||GC_STATUS_APPLIED||CO_SINGLE_QUOTE||','
											 ||quote_nullable(null)||','
											 ||quote_nullable(null)||','
											 ||CO_SINGLE_QUOTE||l_apply_method||CO_SINGLE_QUOTE||','
											 ||co_single_quote||SBS_UTIL.GET_AUDIT_USER()||co_single_quote||',1)';
				ELSE
					   L_SQL_TXT:='INSERT INTO '||l_version_table||' (
																	DESCRIPTION,
																	VERSION,
																	COMMENTS,
																	APPLIED_DATE,
																	STATUS,
																	VERIFICATION_DATE,
																	VERIFIED_BY_USERNAME,
																	APPLY_METHOD,
																	APPLIED_BY,
																	IS_CURRENT)
								VALUES ('
										 ||co_single_quote||l_description||co_single_quote||','
										 ||co_single_quote||l_version||co_single_quote||','
										 ||CO_SINGLE_QUOTE||l_comments||CO_SINGLE_QUOTE||','
										 ||CO_SINGLE_QUOTE||current_timestamp||CO_SINGLE_QUOTE||','
										 ||CO_SINGLE_QUOTE||GC_STATUS_APPLIED||CO_SINGLE_QUOTE||','
										 ||quote_nullable(null)||','
										 ||quote_nullable(null)||','
										 ||CO_SINGLE_QUOTE||l_apply_method||CO_SINGLE_QUOTE||','
										 ||co_single_quote||SBS_UTIL.GET_AUDIT_USER()||co_single_quote||',1)';
				END IF;

                EXECUTE L_SQL_TXT;
            ELSE
				RAISE EXCEPTION USING ERRCODE = 50001;
            END IF;
        ELSE
			RAISE EXCEPTION USING ERRCODE = 50002;
        END IF;
    END IF;
EXCEPTION
    WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;	
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in procedure :- log_apply(),Version already exist. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	
    WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;	
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in procedure :- log_apply(),Invalid value for apply method. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);

    WHEN OTHERS THEN 
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in procedure :- log_apply(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
