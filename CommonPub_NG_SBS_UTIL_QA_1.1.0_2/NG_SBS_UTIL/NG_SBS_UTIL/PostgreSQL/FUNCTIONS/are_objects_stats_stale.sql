CREATE OR REPLACE FUNCTION sbs_util.are_objects_stats_stale 
(
	in_ownname        VARCHAR,
	in_tabname        VARCHAR
)
RETURNS VARCHAR AS
$$
DECLARE
/* --------------------------------------------------------------------------
	-- Purpose : Purpose of this is to verify whether a given Table in a particular Schema can be analyzed.
    -- Parameter Details
	-- in_ownname : Name of the Schema where the Table is present and is to be analyzed. It will return 'Y' 
	--               when modified record count is greater than analyze threshold count
	-- in_tabname  : Name of the Table which is to be analyzed.
   --------------------------------------------------------------------------------
   -- Name: are_objects_stats_stale
   --------------------------------------------------------------------------------
   --
   -- Description:  are_objects_stats_stale
   --
   --------------------------------------------------------------------------------
   -- RefNo Name            JIRA NO 		Date     Description of change
   -- ----- ---------------- -------- ---------------------------------------------
   -- 	1	Sakshi Jain	 CSPUBCC-4304	03/05/2021	are_objects_stats_stale() initial draft
   --   2   Akshay       CSPUBCC-4533   06/04/2021  Handle empty string for input parameters.
*/   
	l_tot_rcrds				INTEGER;
	l_modified_rcrds		INTEGER;
	l_analyze_threshold		INTEGER;
	l_ret					VARCHAR(1):='N';
	l_format_call_stack		TEXT;
Begin
     
	IF (in_ownname IS NULL OR in_ownname = '') THEN
		RAISE EXCEPTION USING ERRCODE = 50001;
	ELSIF (in_tabname IS NULL OR in_tabname = '') THEN
		RAISE EXCEPTION USING ERRCODE = 50002;
	END IF;
	 
	SELECT n_live_tup,n_mod_since_analyze 
	  INTO l_tot_rcrds, l_modified_rcrds
	  FROM pg_stat_all_tables 
	 WHERE schemaname = LOWER(in_ownname) AND relname = LOWER(in_tabname);
	 
	SELECT SUM(CASE WHEN name='autovacuum_analyze_scale_factor' THEN
	       setting::NUMERIC*l_tot_rcrds ELSE
		   setting::NUMERIC END)
	  INTO l_analyze_threshold 
	  FROM pg_settings 
	  WHERE name IN ('autovacuum_analyze_scale_factor','autovacuum_analyze_threshold');
	  
	RAISE info '% : %', l_analyze_threshold , l_modified_rcrds; 
	
	If l_modified_rcrds>l_analyze_threshold THEN
		l_ret :='Y';
	END IF;
	
	RETURN l_ret;
EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in function :- are_objects_stats_stale(), input parameter in_ownname cannot have null or empty value. ', CHR(10), ' SQLERRM :-  ', SQLERRM),
				in_show_stack 		=> TRUE
			);			
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in function :- are_objects_stats_stale(), input parameter in_tabname cannot have null or empty value. ', CHR(10), ' SQLERRM :-  ', SQLERRM),
				in_show_stack 		=> TRUE
			);
	WHEN OTHERS THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack, 
				in_error_code		=> SQLSTATE, 
				in_error_message	=> CONCAT(' Error in function :- are_objects_stats_stale(), ', CHR(10), ' SQLERRM:', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$$ LANGUAGE plpgsql;