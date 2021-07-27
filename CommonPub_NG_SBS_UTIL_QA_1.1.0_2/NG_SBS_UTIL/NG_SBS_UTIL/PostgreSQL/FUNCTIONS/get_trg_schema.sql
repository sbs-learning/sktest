CREATE OR REPLACE FUNCTION sbs_util.get_trg_schema()
RETURNS VARCHAR
LANGUAGE 'plpgsql'
AS
$BODY$
    --------------------------------------------------------------------------------
    -- Name: GET_TRG_SCHEMA
    --------------------------------------------------------------------------------
    --
    -- Description:  Returns comma separated name if more than one of the TARGET schemas on basis of role.
    --
    --------------------------------------------------------------------------------
	  -- RefNo Name            JIRA NO 		Date     	Description of change
      -- ----- ---------------- -------- ---------------------------------------------
      -- 	1	Kalyan	     CSPUBCC-4484	07/05/2021	get schema name initial draft
DECLARE
	l_trg_schema_name	VARCHAR(1000);
	l_format_call_stack TEXT;
BEGIN
  
	SELECT string_agg(grantee::VARCHAR, ',') INTO l_trg_schema_name
	FROM
		(
			select nspname as grantee from pg_roles pr, pg_auth_members pam, pg_namespace pn
		 where pr.oid = pam.roleid and pam.member = pn.nspowner and pr.rolname = 'publish_target_rl'
		) A;

    RETURN l_trg_schema_name;
EXCEPTION
	WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack, 
				in_error_code 		=> SQLSTATE, 
				in_error_message 	=> CONCAT(' Error in function :- get_trg_schema() ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack 		=> TRUE
			);
  END;
$BODY$;