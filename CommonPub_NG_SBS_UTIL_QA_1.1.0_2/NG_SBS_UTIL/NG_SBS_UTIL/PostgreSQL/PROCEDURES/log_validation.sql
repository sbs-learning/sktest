CREATE OR REPLACE PROCEDURE sbs_util.log_validation
(
	in_version			CHARACTER VARYING,
	in_description		CHARACTER VARYING,
	in_comment			CHARACTER VARYING,
	in_version_table	CHARACTER VARYING,
	in_version_seq		CHARACTER VARYING
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE

      -------------------------------------------------------------------------------------------------
      -- Name : log_validation                                                             
      -------------------------------------------------------------------------------------------------
      -- Description : Logs the validation (pre-check) for a version in the specified version table
      --
      -- Logic : Verify the version is in a suitable state, and either insert or update
      --         into Version table
      --
      --Input Parameter details
      --IN_VERSION        - Version to which the tool will be upgraded
      --IN_DESCRIPTION    - Description
      --IN_COMMENT        - Comment about the release
      --IN_VERSION_TABLE  - Version table of the tool
      --IN_VERSION_SEQ    - Sequence on Version table of the tool
	  -----------------------------------------------------------------------------------------------------
      -- RefNo Name            JIRA NO 		Date     Description of change
      -- ----- ---------------- -------- ------------------------------------------------------------------
      -- 	1	Akshay	     CSPUBCC-4301	04/08/2021	log_validation() initial draft
      -- 	2	Akshay	     CSPUBCC-4383	04/15/2021	Added variable l_version, l_description, l_comment,
      --                                                l_version_table, l_version_seq and lower function in query.
      ------------------------------------------------------------------------------------------------------
	  
    l_status                VARCHAR(30);
    l_sql_txt               VARCHAR(32767);
	co_single_quote         VARCHAR(1) DEFAULT CHR(39);
	gc_status_validated     VARCHAR(20) DEFAULT 'VALIDATED';
	gc_apply_method_upgrade VARCHAR(20) DEFAULT 'UPGRADE';
	l_version               VARCHAR(50);
	l_description           VARCHAR(100);
	l_comment               VARCHAR(500);
	l_version_table         VARCHAR(100);
	l_version_seq           VARCHAR(100);
	l_format_call_stack     Text;
	
BEGIN
	l_version       := TRIM(IN_VERSION);
	l_description   := TRIM(IN_DESCRIPTION);
	l_comment       := TRIM(IN_COMMENT);
	l_version_table := LOWER(TRIM(IN_VERSION_TABLE));
	l_version_seq   := LOWER(TRIM(IN_VERSION_SEQ));
	 
    BEGIN
    --Fetch the status of version , i.e. VALIDATED , APPLIED or NULL

        l_sql_txt:='SELECT status
                    FROM '||l_version_table||
                    ' WHERE version ='||co_single_quote||l_version||co_single_quote;

		EXECUTE L_SQL_TXT INTO strict L_STATUS;

	EXCEPTION
		WHEN NO_DATA_FOUND THEN
			l_status := NULL;
    END;
      --if there is no entry in the version table for given version

    IF l_status IS NULL THEN
	IF(l_version_seq is  NOT NULL) THEN
        l_sql_txt:='INSERT INTO '||l_version_table||'(VERSION_ID,
                                                       DESCRIPTION,
                                                       VERSION,
                                                       COMMENTS,
                                                       APPLIED_DATE,
                                                       STATUS,
                                                       VERIFICATION_DATE,
                                                       VERIFIED_BY_USERNAME,
                                                       APPLY_METHOD,
                                                       APPLIED_BY)
                    VALUES (nextval('''||l_version_seq||''')'||','
                             ||CO_SINGLE_QUOTE||l_description||CO_SINGLE_QUOTE||','
                             ||CO_SINGLE_QUOTE||l_version||CO_SINGLE_QUOTE||','
                             ||CO_SINGLE_QUOTE||l_comment||CO_SINGLE_QUOTE||','
                             ||quote_nullable(null)||','
                             ||CO_SINGLE_QUOTE||GC_STATUS_VALIDATED||CO_SINGLE_QUOTE||','
                             ||CO_SINGLE_QUOTE||current_timestamp||CO_SINGLE_QUOTE||','
                             ||CO_SINGLE_QUOTE||SBS_UTIL.GET_AUDIT_USER ()||CO_SINGLE_QUOTE||','
                             ||CO_SINGLE_QUOTE||GC_APPLY_METHOD_UPGRADE||CO_SINGLE_QUOTE||','
                             ||quote_nullable(null)||')';
	ELSE
		l_sql_txt:='INSERT INTO '||l_version_table||'(
                                                       DESCRIPTION,
                                                       VERSION,
                                                       COMMENTS,
                                                       APPLIED_DATE,
                                                       STATUS,
                                                       VERIFICATION_DATE,
                                                       VERIFIED_BY_USERNAME,
                                                       APPLY_METHOD,
                                                       APPLIED_BY)
                    VALUES ('
                             ||CO_SINGLE_QUOTE||l_description||CO_SINGLE_QUOTE||','
                             ||CO_SINGLE_QUOTE||l_version||CO_SINGLE_QUOTE||','
                             ||CO_SINGLE_QUOTE||l_comment||CO_SINGLE_QUOTE||','
                             ||quote_nullable(null)||','
                             ||CO_SINGLE_QUOTE||GC_STATUS_VALIDATED||CO_SINGLE_QUOTE||','
                             ||CO_SINGLE_QUOTE||current_timestamp||CO_SINGLE_QUOTE||','
                             ||CO_SINGLE_QUOTE||SBS_UTIL.GET_AUDIT_USER ()||CO_SINGLE_QUOTE||','
                             ||CO_SINGLE_QUOTE||GC_APPLY_METHOD_UPGRADE||CO_SINGLE_QUOTE||','
                             ||quote_nullable(null)||')';
	END IF;
        EXECUTE L_SQL_TXT;

    ELSE
       --if the status is VALIDATED  in the version table for given version , update the verification date
        IF l_status = gc_status_validated THEN
            L_SQL_TXT:='UPDATE '||l_version_table||
                       ' SET description = '||CO_SINGLE_QUOTE||l_description||CO_SINGLE_QUOTE||',
                       comments = '||co_single_quote||l_comment||co_single_quote||',
                       verification_date = current_timestamp,
                       verified_by_username = '||co_single_quote||sbs_util.get_audit_user()||co_single_quote||
                       'WHERE version = '||co_single_quote||l_version||co_single_quote;
					   RAISE INFO 'Executing: %',L_SQL_TXT;
            EXECUTE L_SQL_TXT;
        ELSE
			RAISE EXCEPTION USING errcode = 50001;
        END IF;
    END IF;
EXCEPTION
    WHEN SQLSTATE '50001' THEN
      GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;	
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT('Error in procedure :- log_validation(),Version requested has already been applied. ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- log_validation(), ', CHR(10), ' SQLERRM:- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;
