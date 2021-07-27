CREATE TABLE pub_admin.pba_param_t
(
/* ----------------------------------------------------------------------------------
      -- Author  : GF8561
      -- Created : 12/05/2021
      -- Purpose : This table is used to keep values against the Parameter Name used in any process.
      -----------------------------------------------------------------------------------
      -- Modification History
      -----------------------------------------------------------------------------------
      -- Ref#   Version#  Name                JIRA#    Date           Change Description
      -----------------------------------------------------------------------------------
      --  1.              Kalyan Kumar                12/05/2021      Initial Version
    ----------------------------------------------------------------------------------- */
	param_nm			CHARACTER VARYING(30)	NOT NULL,
	param_value_txt		CHARACTER VARYING(4000)	NOT NULL,
	param_dsc			CHARACTER VARYING(4000)	NOT NULL,
	rcrd_create_user_id	CHARACTER VARYING(30) NOT NULL,
	rcrd_create_ip 		VARCHAR(30) NOT NULL,
	rcrd_create_ts		TIMESTAMPTZ NOT NULL,
	rcrd_updt_user_id	CHARACTER VARYING(30),
	rcrd_updt_ip 		VARCHAR(30),
	rcrd_updt_ts		TIMESTAMPTZ,
	CONSTRAINT pba_param_t_pkey PRIMARY KEY (param_nm)
);
	COMMENT ON COLUMN "pub_admin"."pba_param_t"."param_nm" IS 'The parameter name, must be unique.';
	COMMENT ON COLUMN "pub_admin"."pba_param_t"."param_value_txt" IS 'The parameter value';
	COMMENT ON COLUMN "pub_admin"."pba_param_t"."param_dsc" IS 'The description describing this parameter''s purpose.';
	COMMENT ON COLUMN "pub_admin"."pba_param_t"."rcrd_create_user_id" IS 'User who inserted this row.';
	COMMENT ON COLUMN "pub_admin"."pba_param_t"."rcrd_create_ip" IS 'IP of the machine from where the row was inserted.';
	COMMENT ON COLUMN "pub_admin"."pba_param_t"."rcrd_create_ts" IS 'Date/time the row was inserted.';
	COMMENT ON COLUMN "pub_admin"."pba_param_t"."rcrd_updt_user_id" IS 'User who applied the latest update to this row.';
	COMMENT ON COLUMN "pub_admin"."pba_param_t"."rcrd_updt_ip" IS 'IP of the machine from where the row was updated.';
	COMMENT ON COLUMN "pub_admin"."pba_param_t"."rcrd_updt_ts" IS 'Date/time of the latest update for this row.';
	COMMENT ON TABLE  "pub_admin"."pba_param_t"  IS 'Parameter table for this PUB_ADMIN instance.';
