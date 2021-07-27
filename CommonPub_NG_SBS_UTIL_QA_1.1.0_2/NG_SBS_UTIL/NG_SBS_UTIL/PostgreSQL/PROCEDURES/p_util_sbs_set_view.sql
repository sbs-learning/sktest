CREATE OR REPLACE PROCEDURE sbs_util.p_util_sbs_set_view(
	         in_fq_view_name  CHARACTER VARYING,  -- in_fq_view_name should be schema_name.view_name
	         in_view_script   TEXT
	         )
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    /* 
	----------------------------------------------------------------------------------------------------------
    -- Name: p_util_sbs_set_view
    ------------------------------------------------------------------------------------------------------------
    --
    -- Description: Purpose of this Procedure is to drop the view if view is already present in metadata table 
	--              and then execute the view script after that if view is not exits then execute the view script and 
	--              insert the view script into the table sbs_view_script and if view is exits then update the 
	--              view script into the table sbs_view_script if fq_vw_name equal to in_fq_view_name.
	--
    -- Parameters: in_fq_view_name, in_view_script
    --
    ----------------------------------------------------------------------------------------------------------------
    -- RefNo Name            JIRA NO 		Date     Description of change
    -----------------------------------------------------------------------------------------------------------------
    -- 	1	Akshay	       CSPUBCC-4506	  6/10/21     p_util_sbs_set_view() initial draft
	--  2   Kalyan Kumar   CSPUBCC-4520   2/07/21     Changes done in logic. Drop view statement was removed and instead it will throw 
	--                                                an error to developers that the view with this name already exists, please drop this before recreating a new one.
	*/
    l_fq_view_name				VARCHAR(200);
	l_vw_schema_nm				VARCHAR(100);
	l_view_nm					VARCHAR(100);
	l_view_exists				SMALLINT;
	l_cnt_view_in_view_table	SMALLINT;
	l_schema_exists             SMALLINT;
	l_view_drop					VARCHAR(200);
	l_format_call_stack			TEXT;
BEGIN
	l_fq_view_name := LOWER(TRIM(in_fq_view_name));
	
	IF (l_fq_view_name IS NULL OR l_fq_view_name = '') THEN
		RAISE EXCEPTION USING errcode = 50002;
	ELSIF (in_view_script IS NULL OR in_view_script = '') THEN
	    RAISE EXCEPTION USING errcode = 50002;
    END IF;
	
    l_vw_schema_nm := SUBSTR(l_fq_view_name , 0, POSITION('.' IN l_fq_view_name));
    l_view_nm  := SUBSTR(l_fq_view_name , POSITION('.' IN l_fq_view_name)+1, LENGTH(l_fq_view_name));
	
	IF ((l_vw_schema_nm IS NULL OR l_vw_schema_nm = '') OR (l_view_nm IS NULL OR l_view_nm = '')) THEN
		RAISE EXCEPTION USING ERRCODE = 50004;
	END IF;
		
	IF (UPPER(in_view_script) NOT LIKE '%CREATE OR REPLACE VIEW%' AND UPPER(in_view_script) NOT LIKE '%CREATE VIEW%') THEN
	   RAISE EXCEPTION USING ERRCODE = 50001;
	END IF;
	
	IF l_vw_schema_nm IS NOT NULL AND l_view_nm IS NOT NULL THEN
          SELECT count(1) INTO STRICT l_schema_exists FROM information_schema.schemata
             WHERE schema_name = l_vw_schema_nm;
	END IF;
	
	IF l_schema_exists = 0 THEN
		RAISE EXCEPTION USING ERRCODE = 50005;
	END IF;
	
    IF l_vw_schema_nm IS NOT NULL AND l_view_nm IS NOT NULL THEN
          SELECT count(1) INTO STRICT l_view_exists FROM information_schema.tables
             WHERE table_schema = l_vw_schema_nm
			 AND table_name = l_view_nm
			 AND table_type = 'VIEW';
	END IF;
	
		IF l_view_exists = 1 THEN
			RAISE EXCEPTION USING ERRCODE = 50003;
	    ELSIF l_view_exists = 0 THEN
			EXECUTE in_view_script;
			
			SELECT COUNT(1) INTO l_cnt_view_in_view_table FROM SBS_UTIL.SBS_VIEW_SCRIPT WHERE LOWER(FQ_VW_NAME) = l_fq_view_name;

		  	--INSERT INTO SBS_UTIL.SBS_VIEW_SCRIPT (FQ_VW_NAME,VW_SCRIPT) VALUES (l_fq_view_name,in_view_script);
			IF l_cnt_view_in_view_table = 0 THEN
		  		INSERT INTO SBS_UTIL.SBS_VIEW_SCRIPT (FQ_VW_NAME,VW_SCRIPT) VALUES (l_fq_view_name,in_view_script);
			ELSIF l_cnt_view_in_view_table = 1 THEN
				UPDATE SBS_UTIL.SBS_VIEW_SCRIPT
				SET VW_SCRIPT = in_view_script
				WHERE FQ_VW_NAME = l_fq_view_name;
			END IF;
		END IF;
EXCEPTION
	WHEN SQLSTATE '50001' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_sbs_set_view(), Input in_view_script parameter is invalid, Please check syntax of view ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50002' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_sbs_set_view(), Input parameter in_fq_view_name/in_view_script cannot accept null or empty value. ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50003' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_sbs_set_view(), The view with name ', in_fq_view_name, ' already exists. Please Drop/Cascade the old view before creating it again. ', CHR(10), ' SQLERRM: ', SQLERRM),
				in_show_stack		=> TRUE
			);
	WHEN SQLSTATE '50004' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
   		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in procedure :- p_util_sbs_set_view(), input parameter fully qualified view name is not passed correctly as schema_name.view_name. ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack 		=> TRUE
			);
	WHEN SQLSTATE '50005' THEN
		GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
   		CALL sbs_util.error_handler
			(
				in_error_stack 		=> l_format_call_stack,
				in_error_code 		=> SQLSTATE,
				in_error_message 	=> CONCAT(' Error in procedure :- p_util_sbs_set_view(), input parameter passed for schema name does not exists. schema_name passed - ', l_vw_schema_nm , CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack 		=> TRUE
			);
    WHEN OTHERS THEN
	  GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in procedure :- p_util_sbs_set_view(), ', CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;