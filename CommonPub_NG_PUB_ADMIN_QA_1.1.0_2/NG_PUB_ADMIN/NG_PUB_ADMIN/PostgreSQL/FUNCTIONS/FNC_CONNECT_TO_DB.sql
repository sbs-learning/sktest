CREATE OR REPLACE FUNCTION pub_admin.fnc_connect_to_db()
    RETURNS CHARACTER VARYING
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
   /* ----------------------------------------------------------------------------------
      -- Author  : GF8561
      -- Created : 01/03/2021
      -- Purpose : This is the function to make db_link connection.
      -----------------------------------------------------------------------------------
      -- Modification History
      -----------------------------------------------------------------------------------
      -- Ref#   Version#  Name                JIRA#    Date           Change Description
      -----------------------------------------------------------------------------------
      --  1.              Kalyan Kumar                01/03/2021      Initial Version
    ----------------------------------------------------------------------------------- */
declare
    l_port                  SMALLINT;
    l_conn_estabilished     TEXT;
    l_host                  VARCHAR;
	str_pos                 SMALLINT;
    l_dbname                VARCHAR;
    l_username              VARCHAR;
    l_password              VARCHAR;
    l_service_name          VARCHAR;
    l_open_connection       SMALLINT;
    l_open_connection_array TEXT;
	l_db_link_name          VARCHAR;
	co_db_password 			CONSTANT VARCHAR(20) DEFAULT 'DB_PASSWORD';
	l_format_call_stack     TEXT;
  BEGIN
    l_host		:= inet_server_addr();
	str_pos		:= strpos(l_host,'/');

	IF str_pos > 0 THEN
		l_host := SUBSTR(l_host, 1, str_pos-1);
	END IF;

	l_port         := inet_server_port();

    l_dbname       := current_database();

	l_username     := current_user;

    --l_db_link_name := 'DBL_' || current_database();
	SELECT pub_admin.fnc_get_db_link_name() INTO STRICT l_db_link_name;
    
	l_password     := pub_admin.get_parameter(co_db_password);

    l_open_connection_array := dblink_get_connections();
    l_open_connection_array := l_open_connection_array || ',';
    l_open_connection_array := REPLACE(REPLACE(l_open_connection_array, '{', ''), '}', '');
    
	SELECT COUNT(1) INTO STRICT l_open_connection
	  FROM regexp_split_to_table(l_open_connection_array,',') 
     WHERE regexp_split_to_table=l_db_link_name;
	    
	IF l_open_connection > 0 THEN
    	RETURN 0;
    ELSE

		SELECT text INTO STRICT l_conn_estabilished
		  FROM dblink_connect(l_db_link_name,
							  'hostaddr=' || quote_nullable(l_host) || ' port =' ||
							  quote_nullable(l_port) || ' dbname=' ||
							  quote_nullable(l_dbname) || ' user=' ||
							  quote_nullable(l_username) || ' password=' ||
							  quote_nullable(l_password)) AS t1(text);
        RETURN 0;
	END IF;
	l_open_connection_array := SUBSTR(l_open_connection_array, strpos(l_open_connection_array, ',') + 1);
EXCEPTION
	WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS l_format_call_stack = PG_EXCEPTION_CONTEXT;
		CALL sbs_util.error_handler
			(
				in_error_stack		=> l_format_call_stack,
				in_error_code		=> SQLSTATE,
				in_error_message	=> CONCAT(' Error in function :- FNC_CONNECT_TO_DB() ' ,CHR(10), ' SQLERRM :- ', SQLERRM),
				in_show_stack		=> TRUE
			);
END;
$BODY$;