-- postgresql

-- cdel.transport_extract definition

-- Drop table

-- DROP TABLE cdel.transport_extract;

CREATE TABLE cdel.transport_extract (
	transport_extract_id int4 NOT NULL,
	extract_name varchar(50) NOT NULL,
	is_active bool NOT NULL DEFAULT true,
	batch_size int8 NULL,
	filter_type bpchar(2) NULL,
	filter_id_col_name varchar(50) NULL,
	filter_id_col_seq int2 NULL,
	filter_id_is_unique bool NULL DEFAULT true,
	filter_dt_col_name varchar(50) NULL,
	filter_dt_col_seq int2 NULL,
	filter_dt_backward_day int2 NOT NULL DEFAULT 0,
	filter_dt_backward_hour int2 NOT NULL DEFAULT 0,
	filter_dt_backward_minute int2 NOT NULL DEFAULT 0,
	filter_id2_is_active bool NOT NULL DEFAULT false,
	filter_id2_col_name varchar(50) NULL,
	filter_id2_col_seq int2 NULL,
	filter_id2_is_unique bool NULL,
	do_full_extract bool NOT NULL DEFAULT false,
	encrypted_column varchar(20) NULL,
	last_extract_max_id int8 NULL,
	last_extract_max_id_id int8 NULL,
	last_extract_max_dt timestamp NULL,
	last_extract_max_dt_id int8 NULL,
	source_server_type varchar(20) NOT NULL,
	source_server_alias varchar(25) NULL,
	source_database_name varchar(50) NOT NULL,
	bucket_name varchar(50) NOT NULL,
	prefix_name varchar(100) NOT NULL,
	s3_ec2_access_credentials varchar(50) NULL,
	run_load bool NOT NULL DEFAULT false,
	transport_load_id int4 NULL,
	extract_script varchar(3000) NOT NULL,
	create_time timestamp NOT NULL,
	update_time timestamp NOT NULL,
	null_to varchar(25) NULL,
	CONSTRAINT transport_extract_filter_type_check CHECK ((filter_type = ANY (ARRAY['id'::bpchar, 'dt'::bpchar]))),
	CONSTRAINT transport_extract_pkey PRIMARY KEY (transport_extract_id),
	CONSTRAINT uc__transport_extract__extract_name UNIQUE (extract_name)
);

-- cdel.transport_extract_history definition

-- Drop table

-- DROP TABLE cdel.transport_extract_history;

CREATE TABLE cdel.transport_extract_history (
	transport_extract_history_id serial4 NOT NULL,
	transport_extract_id int4 NOT NULL,
	group_id int2 NOT NULL,
	extract_name varchar(50) NOT NULL,
	job_id int8 NOT NULL,
	seq_nbr int4 NOT NULL,
	filter_type bpchar(2) NOT NULL,
	batch_size int4 NULL,
	extract_row_count int8 NULL,
	file_size int8 NOT NULL,
	file_size_uncompressed int8 NULL,
	extract_dt_min_value timestamp NULL,
	extract_dt_min_value_id int8 NULL,
	extract_dt_max_value timestamp NULL,
	extract_dt_max_value_id int8 NULL,
	extract_id_min_value int8 NULL,
	extract_id_min_value_id int8 NULL,
	extract_id_max_value int8 NULL,
	extract_id_max_value_id int8 NULL,
	batch_start_time timestamp NULL,
	batch_end_time timestamp NULL,
	extract_start_time timestamp NULL,
	extract_end_time timestamp NULL,
	compress_start_time timestamp NULL,
	compress_end_time timestamp NULL,
	create_time timestamp NOT NULL,
	update_time timestamp NOT NULL,
	bucket_name varchar(50) NULL,
	prefix_name varchar(100) NULL,
	file_timestamp timestamp NULL,
	file_name varchar(150) NULL,
	run_load bool NULL,
	extract_script varchar(5000) NULL,
	status varchar(500) NULL,
	CONSTRAINT transport_extract_history_pkey PRIMARY KEY (transport_extract_history_id)
);
CREATE INDEX transport_extract_extract_id ON cdel.transport_extract_history USING btree (transport_extract_id);
CREATE INDEX transport_extract_history__extract_start_time ON cdel.transport_extract_history USING btree (extract_start_time);

-- cdel.transport_load definition

-- Drop table

-- DROP TABLE cdel.transport_load;

CREATE TABLE cdel.transport_load (
	transport_load_id int4 NOT NULL,
	load_name varchar(50) NOT NULL,
	is_active bool NOT NULL DEFAULT true,
	is_file_overwrite bool NOT NULL DEFAULT false,
	delete_file_after_load bool NOT NULL DEFAULT false,
	target_server_alias varchar(35) NULL,
	target_server_type varchar(20) NOT NULL,
	bucket_name varchar(50) NULL,
	prefix_name varchar(100) NULL,
	s3_ec2_access_credentials varchar(50) NULL,
	s3_redshift_access_credentials varchar(50) NULL,
	use_transport_extract bool NOT NULL DEFAULT false,
	transport_extract_id int4 NULL,
	last_load_extract_history_id int4 NULL,
	last_load_file_timestamp timestamp NULL,
	last_load_file_name varchar(250) NULL,
	load_script varchar(3000) NOT NULL,
	create_time timestamp NOT NULL,
	update_time timestamp NOT NULL,
	check_back_days int4 NOT NULL DEFAULT 0,
	CONSTRAINT transport_load_pkey PRIMARY KEY (transport_load_id),
	CONSTRAINT uc__transport_load__load_name UNIQUE (load_name)
);

-- cdel.transport_load_history definition

-- Drop table

-- DROP TABLE cdel.transport_load_history;

CREATE TABLE cdel.transport_load_history (
	transport_load_history_id serial4 NOT NULL,
	transport_load_id int4 NOT NULL,
	group_id int2 NOT NULL,
	load_name varchar(50) NOT NULL,
	job_id int8 NOT NULL,
	seq_nbr int4 NOT NULL,
	use_transport_extract bool NULL,
	transport_extract_id int4 NULL,
	transport_extract_job_id int8 NULL,
	transport_extract_seq_nbr int4 NULL,
	file_size int8 NULL,
	file_timestamp timestamp NULL,
	load_row_count int8 NULL,
	load_start_time timestamp NULL,
	load_end_time timestamp NULL,
	bucket_name varchar(50) NULL,
	prefix_name varchar(100) NULL,
	file_name varchar(250) NULL,
	target_server_alias varchar(35) NULL,
	target_server_name varchar(100) NULL,
	create_time timestamp NOT NULL,
	update_time timestamp NOT NULL,
	status varchar(1000) NULL,
	load_script varchar(5000) NULL,
	CONSTRAINT transport_load_history_pkey PRIMARY KEY (transport_load_history_id)
);
CREATE INDEX transport_load_history__transport_extract_history ON cdel.transport_load_history USING btree (transport_extract_id, job_id, transport_extract_seq_nbr, transport_extract_job_id);
CREATE INDEX transport_load_history_load_id_group_id ON cdel.transport_load_history USING btree (transport_load_id, group_id, load_start_time);
CREATE INDEX transport_load_history_load_id_start_time ON cdel.transport_load_history USING btree (transport_load_id, load_start_time);

-- cdel.process_task definition

-- Drop table

-- DROP TABLE cdel.process_task;

CREATE TABLE cdel.process_task (
	task_id int4 NOT NULL,
	is_active bool NOT NULL DEFAULT true,
	task_name varchar(50) NOT NULL,
	description varchar(250) NULL DEFAULT NULL::character varying,
	server_type varchar(20) NOT NULL,
	server_name varchar(100) NOT NULL,
	database_name varchar(50) NOT NULL,
	create_time timestamp NOT NULL DEFAULT now(),
	update_time timestamp NOT NULL DEFAULT now(),
	server_alias varchar(35) NULL,
	warehouse_name varchar(50) NULL,
	CONSTRAINT process_task_pkey PRIMARY KEY (task_id),
	CONSTRAINT uc__process_task__task_name UNIQUE (task_name)
);

-- cdel.process_history definition

-- Drop table

-- DROP TABLE cdel.process_history;

CREATE TABLE cdel.process_history (
	process_history_id serial4 NOT NULL,
	job_id int8 NULL,
	group_id int2 NULL,
	task_id int4 NOT NULL,
	step_id int4 NOT NULL,
	row_count int8 NULL,
	start_time timestamp NOT NULL,
	end_time timestamp NULL,
	create_time timestamp NOT NULL DEFAULT now(),
	update_time timestamp NOT NULL DEFAULT now(),
	status_message varchar(500) NULL DEFAULT NULL::character varying,
	step_script varchar(8000) NULL DEFAULT NULL::character varying,
	CONSTRAINT process_history_pkey PRIMARY KEY (process_history_id)
);
CREATE INDEX process_history_start_time ON cdel.process_history USING btree (start_time, task_id, group_id);
CREATE INDEX process_history_task_id ON cdel.process_history USING btree (task_id, group_id, start_time);

-- cdel.process_step definition

-- Drop table

-- DROP TABLE cdel.process_step;

CREATE TABLE cdel.process_step (
	step_id int4 NOT NULL,
	task_id int4 NOT NULL,
	seq_nbr int2 NOT NULL,
	is_active bool NOT NULL DEFAULT true,
	step_name varchar(100) NOT NULL,
	description varchar(250) NULL DEFAULT NULL::character varying,
	step_script varchar(8000) NULL DEFAULT NULL::character varying,
	create_time timestamp NOT NULL DEFAULT now(),
	update_time timestamp NOT NULL DEFAULT now(),
	skip_task_no_data bool NOT NULL DEFAULT false,
	allow_loop bool NOT NULL DEFAULT false,
	CONSTRAINT process_step_pkey PRIMARY KEY (step_id),
	CONSTRAINT process_step_uq_task_seq UNIQUE (task_id, seq_nbr)
);

-- cdel.process_group_task definition

-- Drop table

-- DROP TABLE cdel.process_group_task;

CREATE TABLE cdel.process_group_task (
	task_id int4 NOT NULL,
	group_id int2 NOT NULL,
	seq int2 NULL,
	is_active bool NOT NULL DEFAULT true,
	description varchar(250) NULL DEFAULT NULL::character varying,
	create_time timestamp NOT NULL DEFAULT now(),
	update_time timestamp NOT NULL DEFAULT now(),
	group_type bpchar(1) NOT NULL DEFAULT 'p'::bpchar,
	CONSTRAINT process_group_task_group_type_check CHECK ((group_type = 'p'::bpchar)),
	CONSTRAINT process_group_task_pkey PRIMARY KEY (task_id, group_id)
);

-- cdel.group_etl definition

-- Drop table

-- DROP TABLE cdel.group_etl;

CREATE TABLE cdel.group_etl (
	group_id int4 NOT NULL,
	seq_nbr int4 NOT NULL,
	etl_id int4 NOT NULL,
	etl_type_id bpchar(1) NOT NULL,
	is_active bool NOT NULL DEFAULT true,
	description varchar(50) NULL,
	create_time timestamp NULL DEFAULT now(),
	update_time timestamp NULL DEFAULT now(),
	CONSTRAINT group_etl_etl_type_id_check CHECK ((etl_type_id = ANY (ARRAY['e'::bpchar, 'l'::bpchar, 'p'::bpchar])))
);

-- cdel.job_history definition

-- Drop table

-- DROP TABLE cdel.job_history;

CREATE TABLE cdel.job_history (
	job_id int8 NOT NULL,
	group_id int4 NULL DEFAULT 0,
	start_time timestamp NULL DEFAULT now(),
	end_time timestamp NULL,
	create_time timestamp NOT NULL DEFAULT now(),
	update_time timestamp NOT NULL DEFAULT now(),
	cmd varchar(150) NULL,
	CONSTRAINT job_history_pkey PRIMARY KEY (job_id)
);

-- cdel.tp_group definition

-- Drop table

-- DROP TABLE cdel.tp_group;

CREATE TABLE cdel.tp_group (
	group_id int2 NOT NULL,
	is_active bool NOT NULL DEFAULT true,
	group_type bpchar(1) NULL,
	group_name varchar(50) NULL,
	description varchar(250) NULL,
	create_time timestamp NOT NULL DEFAULT now(),
	update_time timestamp NOT NULL DEFAULT now(),
	CONSTRAINT tp_group_group_type_check CHECK ((group_type = ANY (ARRAY['e'::bpchar, 'l'::bpchar, 'p'::bpchar]))),
	CONSTRAINT tp_group_pkey PRIMARY KEY (group_id)
);

-- cdel.transport_group_object definition

-- Drop table

-- DROP TABLE cdel.transport_group_object;

CREATE TABLE cdel.transport_group_object (
	object_id int4 NOT NULL,
	group_id int2 NOT NULL,
	seq int2 NULL,
	is_active bool NOT NULL DEFAULT true,
	description varchar(250) NULL DEFAULT NULL::character varying,
	create_time timestamp NOT NULL,
	update_time timestamp NOT NULL,
	group_type bpchar(1) NOT NULL DEFAULT 't'::bpchar,
	CONSTRAINT transport_group_object_group_type_check CHECK ((group_type = ANY (ARRAY['e'::bpchar, 'l'::bpchar]))),
	CONSTRAINT transport_group_object_pkey PRIMARY KEY (object_id, group_id)
);



------------------- OUTDATED BELOW ----------------------


-- cdel.transport_object_s3 definition

-- Drop table

-- DROP TABLE cdel.transport_object_s3;

CREATE TABLE cdel.transport_object_s3 (
	transport_object_id int4 NOT NULL,
	is_active bool NOT NULL DEFAULT true,
	object_name varchar(50) NOT NULL,
	bucket_name varchar(50) NOT NULL,
	prefix_name varchar(255) NULL,
	target_server_type varchar(20) NOT NULL,
	target_server_alias varchar(25) NOT NULL,
	loading_script varchar(4000) NOT NULL,
	create_time timestamp NOT NULL DEFAULT now(),
	update_time timestamp NOT NULL DEFAULT now(),
	redshift_copy_credentials varchar(50) NULL,
	last_file_timestamp timestamp NULL,
	last_file_name varchar(100) NULL,
	CONSTRAINT transport_object_s3_pkey PRIMARY KEY (transport_object_id),
	CONSTRAINT uc__transport_object_s3__object_name UNIQUE (object_name)
);

-- cdel.transport_s3_history definition

-- Drop table

-- DROP TABLE cdel.transport_s3_history;

CREATE TABLE cdel.transport_s3_history (
	transport_history_id serial4 NOT NULL,
	job_id int8 NOT NULL,
	transport_object_id int4 NOT NULL,
	object_name varchar(50) NOT NULL,
	seq_nbr int4 NOT NULL,
	file_count_total int4 NOT NULL,
	file_count_remain int4 NOT NULL,
	bucket_name varchar(50) NOT NULL,
	prefix_name varchar(255) NULL,
	file_prefix_name varchar(50) NULL,
	file_name varchar(255) NULL,
	file_size int8 NULL,
	file_timestamp timestamp NULL,
	load_start_time timestamp NULL,
	load_end_time timestamp NULL,
	load_rowcount int8 NULL,
	load_status varchar(1000) NULL,
	create_time timestamp NOT NULL DEFAULT now(),
	update_time timestamp NOT NULL DEFAULT now(),
	CONSTRAINT transport_s3_history_pkey PRIMARY KEY (transport_history_id)
);



------------------- VIEWs may be outdated ----------------------

-- cdel.transport_history source

CREATE OR REPLACE VIEW cdel.transport_history
AS SELECT e.transport_extract_history_id AS transport_history_id,
    e.transport_extract_id AS transport_object_id,
    e.group_id,
    e.extract_name AS object_name,
    e.job_id,
    e.seq_nbr,
    e.filter_type,
    e.batch_size,
    e.extract_row_count,
    e.file_size,
    e.extract_dt_min_value,
    e.extract_dt_min_value_id,
    e.extract_dt_max_value,
    e.extract_dt_max_value_id,
    e.extract_id_min_value,
    e.extract_id_min_value_id,
    e.extract_id_max_value,
    e.extract_id_max_value_id,
    e.batch_start_time,
    e.batch_end_time,
    e.extract_start_time,
    e.extract_end_time,
    e.compress_start_time,
    e.compress_end_time,
    e.create_time,
    e.update_time,
    e.file_name,
    e.extract_script,
    l.load_row_count,
    l.load_start_time,
    l.load_end_time,
    NULL::character varying(1) AS file_path
   FROM cdel.transport_extract_history e
     LEFT JOIN cdel.transport_load_history l ON e.transport_extract_id = l.transport_extract_id AND e.job_id = l.job_id AND e.seq_nbr = l.transport_extract_seq_nbr AND e.job_id = l.transport_extract_job_id;


 -- cdel.transport_object source

CREATE OR REPLACE VIEW cdel.transport_object
AS SELECT e.transport_extract_id AS transport_object_id,
    e.extract_name AS object_name,
    e.is_active,
    e.batch_size,
    e.filter_type,
    e.filter_id_col_name,
    e.filter_id_col_seq,
    e.filter_id_is_unique,
    e.filter_dt_col_name,
    e.filter_dt_col_seq,
    e.filter_dt_backward_day,
    e.filter_dt_backward_hour,
    e.filter_dt_backward_minute,
    NULL::bigint AS last_load_max_id,
    NULL::timestamp without time zone AS last_load_max_dt,
    NULL::bigint AS last_load_max_dt_id,
    true AS delete_file_after_load,
    l.delete_file_after_load AS delete_s3_file_after_load,
    e.encrypted_column,
    e.source_server_type,
    e.source_server_alias,
    NULL::character varying(50) AS source_server_name,
    e.source_database_name,
    NULL::character varying(50) AS source_schema_name,
    NULL::character varying(50) AS source_object_name,
    NULL::character varying(50) AS source_fq_object_name,
    l.target_server_type,
    l.target_server_alias,
    NULL::character varying(50) AS target_server_name,
    NULL::character varying(50) AS target_database_name,
    NULL::character varying(50) AS target_object_name,
    NULL::character varying(50) AS target_fq_object_name,
    e.create_time,
    e.update_time,
    e.do_full_extract AS do_reload,
    e.filter_id2_is_active,
    e.filter_id2_col_name,
    e.filter_id2_col_seq,
    e.filter_id2_is_unique,
    NULL::bigint AS last_load_max_id_id,
    e.extract_script,
    l.load_script
   FROM cdel.transport_extract e
     LEFT JOIN cdel.transport_load l ON e.transport_load_id = l.transport_load_id;


-- cdel.v_process_history_display source

CREATE OR REPLACE VIEW cdel.v_process_history_display
AS SELECT h.job_id,
    h.group_id,
    NULL::smallint AS task_seq,
    t.task_id,
    t.task_name,
    s.step_id,
    s.seq_nbr AS step_seq,
    s.step_name,
    h.start_time AS start_time_utc,
    h.end_time AS end_time_utc,
    date_part('epoch'::text, h.end_time - h.start_time) AS total_sec,
    h.row_count,
    h.status_message,
    g.group_name,
    COALESCE(g.group_type, 'p'::bpchar) AS group_type
   FROM cdel.process_history h
     JOIN cdel.process_step s ON h.step_id = s.step_id
     JOIN cdel.process_task t ON s.task_id = t.task_id
     LEFT JOIN cdel.tp_group g ON h.group_id = g.group_id;


-- cdel.v_transport_history_display source

CREATE OR REPLACE VIEW cdel.v_transport_history_display
AS SELECT h.transport_history_id,
    h.job_id,
    h.group_id,
    h.transport_object_id AS object_id,
    h.object_name,
    h.seq_nbr,
    h.extract_row_count,
    h.file_size AS file_size_byte,
    h.batch_start_time,
    h.batch_end_time,
    h.extract_start_time,
    h.extract_end_time,
    h.compress_start_time,
    h.compress_end_time,
    h.load_start_time,
    h.load_end_time,
    date_part('epoch'::text, h.extract_end_time - h.extract_start_time) AS extract_sec,
    date_part('epoch'::text, h.load_start_time - h.extract_end_time) AS file_process_sec,
    date_part('epoch'::text, h.load_end_time - h.load_start_time) AS load_sec,
    date_part('epoch'::text, h.load_end_time - h.extract_start_time) AS total_sec,
    h.filter_type,
    h.extract_dt_min_value,
    h.extract_dt_max_value,
    h.extract_dt_max_value_id,
    h.extract_id_min_value,
    h.extract_id_max_value,
    h.extract_id_max_value_id,
    o.source_server_type,
    o.source_server_name,
    o.target_server_type,
    o.target_server_name,
    g.group_name,
    COALESCE(g.group_type, 't'::bpchar) AS group_type
   FROM cdel.transport_history h
     LEFT JOIN cdel.transport_object o ON h.transport_object_id = o.transport_object_id
     LEFT JOIN cdel.tp_group g ON h.group_id = g.group_id;


-- cdel.v_transport_object_statement source

CREATE OR REPLACE VIEW cdel.v_transport_object_statement
AS WITH upd AS (
         SELECT transport_object.object_name,
            'upd'::character varying AS type,
            transport_object.source_server_alias,
            transport_object.target_server_alias,
            concat('update cdel.transport_object
set extract_script = ''', replace(transport_object.extract_script::text, ''''::text, ''''''::text), '''
,load_script = ''', replace(transport_object.load_script::text, ''''::text, ''''''::text), '''
where object_name = ''', transport_object.object_name, '''
and target_server_type = ''', transport_object.target_server_type, '''
;
') AS mscript
           FROM cdel.transport_object
          WHERE transport_object.is_active = true
        ), ins AS (
         SELECT transport_object.object_name,
            'ins'::character varying AS type,
            transport_object.source_server_alias,
            transport_object.target_server_alias,
            concat(('----------     '::text || transport_object.object_name::text) || '     ----------

insert into cdel.transport_object (
transport_object_id,object_name,is_active,batch_size,filter_type,
filter_id_col_name,filter_id_col_seq,filter_id_is_unique,filter_dt_col_name,filter_dt_col_seq, do_reload,
filter_dt_backward_hour, filter_dt_backward_minute,source_server_alias,
source_server_type,source_server_name,source_database_name,source_schema_name,source_object_name,source_fq_object_name,
target_server_alias,target_server_type,target_server_name,target_database_name,target_object_name,target_fq_object_name,
delete_file_after_load,delete_s3_file_after_load,encrypted_column,
last_load_max_id,last_load_max_dt,last_load_max_dt_id,create_time,update_time,extract_script,load_script
)
values ((select coalesce(max(transport_object_id),0)+1 from cdel.transport_object),
'::text, '''', transport_object.object_name, ''', ',
                CASE transport_object.is_active
                    WHEN true THEN 'true'::text
                    ELSE 'false'::text
                END, ', ', transport_object.batch_size, ', ',
                CASE
                    WHEN transport_object.filter_type IS NULL THEN 'NULL'::text
                    ELSE concat('''', transport_object.filter_type, '''')
                END, ',',
                CASE
                    WHEN transport_object.filter_id_col_name IS NULL THEN 'NULL'::text
                    ELSE concat('''', transport_object.filter_id_col_name, '''')
                END, ', ', transport_object.filter_id_col_seq, ', ',
                CASE transport_object.filter_id_is_unique
                    WHEN true THEN 'true'::text
                    ELSE 'false'::text
                END, ', ',
                CASE
                    WHEN transport_object.filter_dt_col_name IS NULL THEN 'NULL'::text
                    ELSE concat('''', transport_object.filter_dt_col_name, '''')
                END, ', ', transport_object.filter_dt_col_seq, ', ',
                CASE transport_object.do_reload
                    WHEN true THEN 'true'::text
                    ELSE 'false'::text
                END, ', ', transport_object.filter_dt_backward_hour, ', ', transport_object.filter_dt_backward_minute, ', ',
                CASE
                    WHEN transport_object.source_server_alias IS NULL THEN 'NULL'::text
                    ELSE concat('''', transport_object.source_server_alias, '''')
                END, ',
', '''', transport_object.source_server_type, ''', ', '''', transport_object.source_server_name, ''', ',
                CASE
                    WHEN transport_object.source_database_name IS NULL THEN 'NULL'::text
                    ELSE concat('''', transport_object.source_database_name, '''')
                END, ', ',
                CASE
                    WHEN transport_object.source_schema_name IS NULL THEN 'NULL'::text
                    ELSE concat('''', transport_object.source_schema_name, '''')
                END, ', ',
                CASE
                    WHEN transport_object.source_object_name IS NULL THEN 'NULL'::text
                    ELSE concat('''', transport_object.source_object_name, '''')
                END, ', ',
                CASE
                    WHEN transport_object.source_fq_object_name IS NULL THEN 'NULL'::text
                    ELSE concat('''', transport_object.source_fq_object_name, '''')
                END, ',
',
                CASE
                    WHEN transport_object.target_server_alias IS NULL THEN 'NULL'::text
                    ELSE concat('''', transport_object.target_server_alias, '''')
                END, ',', '''', transport_object.target_server_type, ''', ', '''', transport_object.target_server_name, ''', ',
                CASE
                    WHEN transport_object.target_database_name IS NULL THEN 'NULL'::text
                    ELSE concat('''', transport_object.target_database_name, '''')
                END, ', ',
                CASE
                    WHEN transport_object.target_object_name IS NULL THEN 'NULL'::text
                    ELSE concat('''', transport_object.target_object_name, '''')
                END, ', ',
                CASE
                    WHEN transport_object.target_fq_object_name IS NULL THEN 'NULL'::text
                    ELSE concat('''', transport_object.target_fq_object_name, '''')
                END, ', 
',
                CASE transport_object.delete_file_after_load
                    WHEN true THEN 'true'::text
                    ELSE 'false'::text
                END, ', ',
                CASE transport_object.delete_s3_file_after_load
                    WHEN true THEN 'true'::text
                    ELSE 'false'::text
                END, ', ',
                CASE
                    WHEN transport_object.encrypted_column IS NULL THEN 'NULL'::text
                    ELSE concat('''', transport_object.encrypted_column, '''')
                END, ', ', 'NULL,', 'NULL,', 'NULL,', 'now(), now()', ', ', ''''',', '''''', ')
;
') AS mscript
           FROM cdel.transport_object
          WHERE transport_object.is_active = true
        ), ascript AS (
         SELECT upd.object_name,
            upd.type,
            upd.source_server_alias,
            upd.target_server_alias,
            upd.mscript
           FROM upd
        UNION ALL
         SELECT ins.object_name,
            ins.type,
            ins.source_server_alias,
            ins.target_server_alias,
            ins.mscript
           FROM ins
        )
 SELECT ascript.object_name,
    ascript.type,
    ascript.source_server_alias,
    ascript.target_server_alias,
    ascript.mscript
   FROM ascript;

