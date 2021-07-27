	CREATE OR REPLACE PROCEDURE sbs_util.error_handler(
		in_error_stack character varying,
		in_error_code character varying,
		in_error_message character varying,
		in_show_stack boolean DEFAULT true)
	LANGUAGE 'plpgsql'
	AS $BODY$
			--------------------------------------------------------------------------------
			-- Name: error_handler
			--------------------------------------------------------------------------------
			--
			-- Description:  error_handler procedure is  used to  raise error in standard format with error message and error code and  error stack.
			-- It requires parameter
			-- in_error_stack character varying -- error stack passed using GET DIAGNOSTIC
			-- in_error_code character varying  -- sql error code passed
			-- in_error_message character varying -- sql error message passed
			-- in_show_stack boolean DEFAULT true -- TRUE to display error stack and FALSE to hide error stack 
			--
			-- --------------------------------------------------------------------------------
		   -- RefNo Name            JIRA NO 		Date     	Description of change
		   -- ----- ---------------- -------- ---------------------------------------------
		   -- 	1	Sakshi Jain	 	CSPUBCC-4310	04/05/2021	error_handler() initial draft
			--------------------------------------------------------------------------------
	DECLARE
		  l_format_call_stack    text;
		  l_format_call_message   text;
		  l_error_code varchar;
	  BEGIN  
			l_format_call_stack := coalesce(in_error_stack,'');
			l_format_call_message := coalesce(in_error_message,'');
			l_error_code := coalesce(in_error_code,'');

				RAISE ' ';

			EXCEPTION

				when others then
				--Reraise error after raise error info
				if(in_show_stack is TRUE) then
					if(l_error_code != sqlstate ) then
					RAISE   INFO '
--- START OF ERROR STACK: ---

Error Code: % ,
Error Message: % ,
Error Stack: %

--- END OF ERROR STACK: ---						

	', l_error_code, l_format_call_message, l_format_call_stack ;

					end if;
					raise;
				else
					if(l_error_code != sqlstate ) then
						RAISE   INFO '
--- START OF ERROR : ---

Error Code: % ,
Error Message: %

--- END OF ERROR : ---					

		', l_error_code, l_format_call_message ;

						end if;
						raise;
				end if;
	   END;
	$BODY$;
