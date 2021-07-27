CREATE TABLE sbs_util.sbs_view_script_hst_t
(
	vw_hst_id				INTEGER GENERATED BY DEFAULT AS IDENTITY,
	vw_id                 	INTEGER,
	fq_vw_name            	VARCHAR(200),
	vw_script             	TEXT,
	rcrd_create_user_id   	VARCHAR(30),
	rcrd_create_ip 	   		VARCHAR(30),
	rcrd_create_ts		   	TIMESTAMPTZ,
	rcrd_dml_type_cd	     VARCHAR(5),
	rcrd_updt_user_id	   	VARCHAR(30),
	rcrd_updt_ip 		   	VARCHAR(30),
	rcrd_updt_ts		   	TIMESTAMPTZ,
    hst_rcrd_create_user_id	VARCHAR(30) NOT NULL, 
    hst_rcrd_create_ip 		VARCHAR(30) NOT NULL,
    hst_rcrd_create_ts   	TIMESTAMPTZ NOT NULL,
	CONSTRAINT sut_sbs_view_script_hst_pkey PRIMARY KEY (vw_hst_id)
);