CREATE TABLE pba_cons_index_t
(
    cons_index_nm character varying(30) NOT NULL,
    cons_index_schema_nm character varying(30) NOT NULL,
    cons_index_table_nm character varying(30) NOT NULL,
    is_cons_or_index character varying(1) NOT NULL,
    cons_index_type character varying(20),
    rcrd_create_ts timestamp without time zone,
    rcrd_create_user_id character varying(30),
    rcrd_updt_ts timestamp without time zone,
    rcrd_updt_user_id character varying(30),
    cons_index_def character varying(4000),
    CONSTRAINT pba_cons_index_pk PRIMARY KEY (cons_index_schema_nm, cons_index_table_nm, cons_index_nm)
);