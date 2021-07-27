	CREATE OR REPLACE PROCEDURE sbs_util.error_handler(
		in_error_code character varying,
		in_error_message character varying)
	LANGUAGE 'plpgsql'
	AS $BODY$
		--------------------------------------------------------------------------------
		-- Name: error_handler
		--------------------------------------------------------------------------------
		--
		-- Description:  error_handler procedure is  used to  raise error in standard format with error message and error code.
		-- It requires parameter
		-- in_error_code character varying  -- sql error code passed
		-- in_error_message character varying -- sql error message passed
		--
		-- --------------------------------------------------------------------------------
	   -- RefNo Name            JIRA NO 		Date     	Description of change
	   -- ----- ---------------- -------- ---------------------------------------------
	   -- 	1	Sakshi Jain	 	CSPUBCC-4310	03/05/2021	error_handler() initial draft
		--------------------------------------------------------------------------------
	DECLARE
		  l_format_call_message   text;
		  l_error_code varchar;
	  BEGIN  
			l_format_call_message := coalesce(in_error_message,'');
			l_error_code := coalesce(in_error_code,'');

				RAISE ' ';

			EXCEPTION
				--Reraise error after raise error info
				when others then
				if(l_error_code != sqlstate ) then
					RAISE   INFO '
--- START OF ERROR : ---

Error Code: % ,
Error Message: %

--- END OF ERROR : ---					

	', l_error_code, l_format_call_message ;

					end if;
					raise;
	END;
	$BODY$;