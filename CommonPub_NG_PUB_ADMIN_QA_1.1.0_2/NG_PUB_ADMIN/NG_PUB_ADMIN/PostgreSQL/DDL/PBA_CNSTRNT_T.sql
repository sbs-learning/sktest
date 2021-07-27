CREATE TABLE pub_admin.pba_cnstrnt_t
(
/* ----------------------------------------------------------------------------------
      -- Author  : GF8561
      -- Created : 22/04/2021
      -- Purpose : This table is used to insert Constraints of tables before they are dropped.
      -----------------------------------------------------------------------------------
      -- Modification History
      -----------------------------------------------------------------------------------
      -- Ref#   Version#  Name                JIRA#    Date           Change Description
      -----------------------------------------------------------------------------------
      --  1.              Kalyan Kumar                22/04/2021      Initial Version
    ----------------------------------------------------------------------------------- */
	
    cnstrnt_nm character varying(100) NOT NULL,
    cnstrnt_schema_nm character varying(100) NOT NULL,
    cnstrnt_parent_table_nm character varying(100) NOT NULL,
    constraint_def character varying(100) NOT NULL,
    rcrd_create_ts timestamptz NOT NULL,
    rcrd_create_user_id character varying(100),
    rcrd_updt_ts timestamptz,
    rcrd_updt_user_id character varying(100),
    cnstrnt_child_table_nm character varying(100) NOT NULL,
    cnstrnt_type character varying(1)
);

   COMMENT ON COLUMN "pub_admin"."pba_cnstrnt_t"."cnstrnt_nm" IS 'Name of the constraint.';
   COMMENT ON COLUMN "pub_admin"."pba_cnstrnt_t"."cnstrnt_schema_nm" IS 'Schema in which this constraint is located.';
   COMMENT ON COLUMN "pub_admin"."pba_cnstrnt_t"."cnstrnt_parent_table_nm" IS 'Table to which this constraint applies.';
   COMMENT ON COLUMN "pub_admin"."pba_cnstrnt_t"."constraint_def" IS 'Full defination of the constraint.';
   COMMENT ON COLUMN "pub_admin"."pba_cnstrnt_t"."rcrd_create_ts" IS 'Date/time the row was inserted.';
   COMMENT ON COLUMN "pub_admin"."pba_cnstrnt_t"."rcrd_create_user_id" IS 'User who inserted this row.';
   COMMENT ON COLUMN "pub_admin"."pba_cnstrnt_t"."rcrd_updt_ts" IS 'Date/time of the latest update for this row.';
   COMMENT ON COLUMN "pub_admin"."pba_cnstrnt_t"."rcrd_updt_user_id" IS 'User who applied the latest update to this row..';
   COMMENT ON COLUMN "pub_admin"."pba_cnstrnt_t"."cnstrnt_child_table_nm" IS 'Name of the child table involved in the constraint.';
   COMMENT ON COLUMN "pub_admin"."pba_cnstrnt_t"."cnstrnt_type" IS 'Type of the constraint, whether it is P, f, u.';
   COMMENT ON TABLE "pub_admin"."pba_cnstrnt_t"  IS 'CONSTRAINT A Constraint that must be re-enabled following a Truncate.';