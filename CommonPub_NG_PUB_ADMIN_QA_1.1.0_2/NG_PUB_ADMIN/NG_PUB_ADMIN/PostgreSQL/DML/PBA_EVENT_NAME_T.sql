 --------------------------------------------------------------------------------
  -- Name: PBA_EVENT_NAME_T.sql 
  -------------------------------------------------------------------------------- 
  -- -- Description:  Required for Data receipt -- 
  -------------------------------------------------------------------------------- 
  -- RefNo Name             Date     Description of change 
  -- ----- ---------------- -------- --------------------------------------------- 
  -- 1     Kalyan Kumar      04/30/2021          Created this script
  -- 2     Kalyan Kumar      07/05/2021          Added Entry for Event ROW_COUNT.
  
INSERT INTO pub_admin.pba_event_name_t(event_constant, event_name, event_name_desc)
	VALUES ('CO_DROP_TABLE', 'DROP TABLE', 'Dropping of Tables')
	ON CONFLICT(event_constant)
	DO NOTHING;

INSERT INTO pub_admin.pba_event_name_t(event_constant, event_name, event_name_desc)
	VALUES ('CO_DROP_PARTITION', 'DROP PARTITION', 'Dropping of Partitions')
	ON CONFLICT(event_constant)
	DO NOTHING;
	
INSERT INTO pub_admin.pba_event_name_t(event_constant, event_name, event_name_desc)
	VALUES ('CO_ANALYZE_TABLE', 'ANALYZE TABLE', 'Analyzing Tables')
	ON CONFLICT(event_constant)
	DO NOTHING;

INSERT INTO pub_admin.pba_event_name_t(event_constant, event_name, event_name_desc)
	VALUES ('CO_ANALYZE_TABLE_N_PARTITION', 'ANALYZE TABLE/PARTITION', 'Analyzing Tables or Partitions')
	ON CONFLICT(event_constant)
	DO NOTHING;
	
INSERT INTO pub_admin.pba_event_name_t(event_constant, event_name, event_name_desc)
	VALUES ('CO_CHECK_NEW_FILE_RECEIVED', 'CONTAINER PROCESS', 'CHECK_NEW_FILE_RECEIVED error discription')
	ON CONFLICT(event_constant)
	DO NOTHING;
	
INSERT INTO pub_admin.pba_event_name_t(event_constant, event_name, event_name_desc)
	VALUES ('CO_HANDLE_CONSTRAINTS', 'HANDLE CONSTRAINT', 'Handling of Constraints')
	ON CONFLICT(event_constant)
	DO NOTHING;
	
INSERT INTO pub_admin.pba_event_name_t(event_constant, event_name, event_name_desc)
	VALUES ('CO_REFRESH_MV', 'REFRESH MV', 'Refresh Materialized View')
	ON CONFLICT(event_constant)
	DO NOTHING;
	
INSERT INTO pub_admin.pba_event_name_t(event_constant, event_name, event_name_desc)
	VALUES ('CO_ADD_RANGE_PARTITION', 'ADD_RANGE_PARTITION', 'Add Range Partition')
	ON CONFLICT(event_constant)
	DO NOTHING;
	
INSERT INTO pub_admin.pba_event_name_t(event_constant, event_name, event_name_desc)
	VALUES ('CO_ROW_COUNT', 'ROW COUNT', 'To Get Row Count')
	ON CONFLICT(event_constant)
	DO NOTHING;