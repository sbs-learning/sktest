 --------------------------------------------------------------------------------
  -- Name: PBA_PARAM_T.sql 
  -------------------------------------------------------------------------------- 
  -- -- Description:  Required for PubAdmin logging and Execution -- 
  -------------------------------------------------------------------------------- 
  -- RefNo Name             Date     Description of change 
  -- ----- ---------------- -------- --------------------------------------------- 
  -- 1     Kalyan Kumar      05/20/2021          Created this script 

CALL PUB_ADMIN.SET_PARAMETER('LOG_MODE', 'ON', 'This is Logging Mode for PubAdmin. If should be kept ON if logging is needed, it should be kept OFF if logging is not needed.');

CALL PUB_ADMIN.SET_PARAMETER('DB_PASSWORD', 'ABC-XYZ', 'Password for Database.');

