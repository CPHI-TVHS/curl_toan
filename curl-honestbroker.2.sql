/*
 * Copyright (c) 2017. The Regents of the University of Colorado.
 * All rights reserved.
 */

-- Create honestbroker user and database
DROP DATABASE IF EXISTS honestbroker;
DROP USER IF EXISTS honestbroker;
CREATE USER honestbroker PASSWORD 'curl2017';
CREATE DATABASE honestbroker WITH OWNER = honestbroker;
GRANT ALL PRIVILEGES ON DATABASE honestbroker TO honestbroker;

-- Connnect to honstbroker database
\c honestbroker
-- Create honestbroker tables and objects
/*
 * Copyright (c) 2017. The Regents of the University of Colorado.
 * All rights reserved.
 */
DROP SEQUENCE IF EXISTS public.hibernate_sequence;

CREATE SEQUENCE public.hibernate_sequence
INCREMENT 1
START 1
MINVALUE 1
MAXVALUE 9223372036854775807
CACHE 1;

ALTER SEQUENCE public.hibernate_sequence
OWNER TO honestbroker;


DROP TABLE IF EXISTS public.error_log;

CREATE TABLE public.error_log
(
  id bigint NOT NULL,
  date timestamp without time zone,
  job_uuid character varying(255) COLLATE pg_catalog."default" NOT NULL,
  message character varying(2000) COLLATE pg_catalog."default",
  step_name character varying(255) COLLATE pg_catalog."default",
  type character varying(255) COLLATE pg_catalog."default",
  CONSTRAINT error_log_pkey PRIMARY KEY (id)
)
WITH (
OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.error_log
  OWNER to honestbroker;


DROP TABLE IF EXISTS public.property;

CREATE TABLE public.property
(
  id bigint NOT NULL,
  property_name character varying(255) COLLATE pg_catalog."default",
  property_value character varying(255) COLLATE pg_catalog."default",
  CONSTRAINT property_pkey PRIMARY KEY (id),
  CONSTRAINT uk_mv1lqgdyatavo82v57vufcfic UNIQUE (property_name)
)
WITH (
OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.property
  OWNER to honestbroker;


DROP TABLE IF EXISTS public.step;

CREATE TABLE public.step
(
  id bigint NOT NULL,
  class_name character varying(255) COLLATE pg_catalog."default" NOT NULL,
  end_time timestamp without time zone,
  input_file character varying(255) COLLATE pg_catalog."default",
  job_uuid character varying(255) COLLATE pg_catalog."default" NOT NULL,
  multithreaded boolean NOT NULL,
  output_file character varying(255) COLLATE pg_catalog."default",
  paused boolean NOT NULL,
  start_time timestamp without time zone,
  status character varying(255) COLLATE pg_catalog."default",
  step_name character varying(255) COLLATE pg_catalog."default" NOT NULL,
  step_order integer NOT NULL,
  CONSTRAINT step_pkey PRIMARY KEY (id)
)
WITH (
OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.step
  OWNER to honestbroker;


DROP TABLE IF EXISTS public.test;

CREATE TABLE public.test
(
  id bigint,
  value character varying(50) COLLATE pg_catalog."default"
)
WITH (
OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.test
  OWNER to honestbroker;

-- Create create_job_schema function
DROP FUNCTION IF EXISTS create_job_schema(character varying, text[], text[], text[], text[], int, boolean);

CREATE OR REPLACE FUNCTION create_job_schema(
    job_schema character varying,
    source_variables text[],
    pair_blocking_variables text[],
    method_blocking_variables text[],
    linkage_variables text[],
    number_of_methods int,
    match_once boolean
)
    RETURNS void
LANGUAGE plpgsql
VOLATILE
COST 100
AS $BODY$
/*
 * Copyright (c) 2017. The Regents of the University of Colorado.
 * All rights reserved.
 */
DECLARE
    sql TEXT;
    blocking_variable TEXT;
BEGIN
    sql = 'DROP SCHEMA IF EXISTS ' || job_schema || ' CASCADE';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'CREATE SCHEMA ' || job_schema;
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'CREATE TABLE ' || job_schema || '.source (' ||
          ' source_id integer PRIMARY KEY,' ||
          ' site_uuid character varying(40) ' ||
          ')';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'CREATE TABLE ' || job_schema || '.merged_source (' ||
          ' uid bigserial PRIMARY KEY,' ||
          ' source_id integer';
    FOR i IN 1 .. array_length(source_variables, 1) LOOP
        sql = sql || ', ' || source_variables[i] || ' text';
    END LOOP;
    sql = sql || ')';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    FOR i IN 1 .. array_length(pair_blocking_variables, 1) LOOP
        IF length(pair_blocking_variables[i]) > 0 THEN
            blocking_variable = pair_blocking_variables[i];
        ELSE
            blocking_variable = 'all';
        END IF;
        sql = 'CREATE TABLE ' || job_schema || '.pair_' || blocking_variable || ' (' ||
              ' id bigserial,'
              ' left_uid bigint,' ||
              ' right_uid bigint,' ||
              ' blocking_variable_value text' ||
              ')';
        RAISE NOTICE '%', sql;
        EXECUTE sql;

        sql = 'CREATE TABLE ' || job_schema || '.pair_' || blocking_variable || '_sample (' ||
              ' id bigint,' ||
              ' left_uid bigint,' ||
              ' right_uid bigint,' ||
              ' blocking_variable_value text' ||
              ')';
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;

    sql = 'CREATE TABLE ' || job_schema || '.block_profile (' ||
          ' blocking_variable_name character varying(50),' ||
          ' block_count integer,' ||
          ' average numeric,' ||
          ' minimum numeric,' ||
          ' maximum numeric,' ||
          ' standard_deviation numeric' ||
          ')';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'CREATE TABLE ' || job_schema || '.weight (' ||
          ' method_index integer,' ||
          ' variable_name character varying(50),' ||
          ' weight numeric' ||
          ')';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'CREATE TABLE ' || job_schema || '.missing_data_rule (' ||
          ' method_index integer,' ||
          ' rule text' ||
          ')';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'CREATE TABLE ' || job_schema || '.result (' ||
          ' method_index integer,' ||
          ' left_uid bigint,' ||
          ' right_uid bigint,' ||
          ' blocking_variable_name character varying(50),'
          ' blocking_variable_value text,' ||
          ' confidence numeric,' ||
          ' match_type character varying(20)' ||
          ')';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'CREATE TABLE ' || job_schema || '.result_profile (' ||
          ' method_index integer,' ||
          ' total integer,' ||
          ' matching integer,' ||
          ' probable_matching integer,' ||
          ' eq_100 integer,' ||
          ' ge_95_lt_100 integer,' ||
          ' ge_90_lt_95 integer,' ||
          ' ge_85_lt_90 integer,' ||
          ' ge_80_lt_85 integer,' ||
          ' lt_80 integer' ||
          ')';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'CREATE TABLE ' || job_schema || '.network_id (' ||
          ' source_id int,' ||
          ' local_id text,' ||
          ' network_id bigint' ||
          ')';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    FOR i IN 1 .. number_of_methods LOOP
        IF length(method_blocking_variables[i]) > 0 THEN
            sql = 'CREATE VIEW ' || job_schema || '.method_' || i::text || '_count AS ' ||
                  'SELECT count(1) as num_recs FROM ' || job_schema || '.pair_' || method_blocking_variables[i];
        ELSE
            sql = 'CREATE VIEW ' || job_schema || '.method_' || i::text || '_count AS ' ||
                  'SELECT count(1) as num_recs FROM ' || job_schema || '.pair_all';
        END IF;

        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;

    FOR i IN 1 .. number_of_methods LOOP
        IF length(method_blocking_variables[i]) > 0 THEN
            sql = 'CREATE VIEW ' || job_schema || '.method_' || i::text || ' AS' ||
                  ' SELECT ' ||
                  i::text || ' AS method_index,' ||
                  ' a.id,' ||
                  ' a.left_uid,' ||
                  ' a.right_uid,' ||
                  ' a.blocking_variable_value';
            FOR j IN 1 .. array_length(linkage_variables, 1) LOOP
                sql = sql || ', b.' || linkage_variables[j] || ' AS left_' || linkage_variables[j];
                sql = sql || ', c.' || linkage_variables[j] || ' AS right_' || linkage_variables[j];
            END LOOP;
            sql = sql || ' FROM ' || job_schema || '.pair_' || method_blocking_variables[i] || ' a' ||
                  ' JOIN ' || job_schema || '.merged_source b' ||
                  ' ON a.left_uid = b.uid AND a.blocking_variable_value = b.' || method_blocking_variables[i] ||
                  ' JOIN ' || job_schema || '.merged_source c ' ||
                  ' ON a.right_uid = c.uid AND a.blocking_variable_value = c.' || method_blocking_variables[i];
        ELSE
            sql = 'CREATE VIEW ' || job_schema || '.method_' || i::text || ' AS' ||
                  ' SELECT ' ||
                  i::text || ' AS method_index,' ||
                  ' b.uid as id,' ||
                  ' b.uid as left_uid,' ||
                  ' c.uid as right_uid,' ||
                  ' '''' blocking_variable_value';
            FOR j IN 1 .. array_length(linkage_variables, 1) LOOP
                sql = sql || ', b.' || linkage_variables[j] || ' AS left_' || linkage_variables[j];
                sql = sql || ', c.' || linkage_variables[j] || ' AS right_' || linkage_variables[j];
            END LOOP;
--            sql = sql || ' FROM ' || job_schema || '.pair_all a' ||
--                  ' JOIN ' || job_schema || '.merged_source b on a.left_uid = b.uid' ||
--                  ' JOIN ' || job_schema || '.merged_source c on a.right_uid = c.uid';
            sql = sql || ' FROM ' || job_schema || '.merged_source b' ||
                  ' JOIN ' || job_schema || '.merged_source c ' ||
                  ' ON b.uid > c.uid';
        END IF;

        IF match_once THEN
            sql = sql || ' WHERE NOT EXISTS (SELECT 1 FROM ' || job_schema || '.result d ' ||
                  'WHERE d.left_uid = b.uid AND d.right_uid = c.uid)';
        END IF;
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;
    
    

    FOR i IN 1 .. number_of_methods LOOP
        IF length(method_blocking_variables[i]) > 0 THEN
            sql = 'CREATE VIEW ' || job_schema || '.method_' || i::text || '_left_sample AS' ||
                  ' SELECT ';
--                  i::text || ' AS method_index,' ||
--                  ' a.left_uid as uid,' ||
--                  ' a.blocking_variable_value';
            FOR j IN 1 .. array_length(linkage_variables, 1) LOOP
                IF j > 1 THEN
                    sql = sql || ', ';
                END IF;
                sql = sql || 'b.' || linkage_variables[j];
            END LOOP;
            sql = sql || ' FROM ' || job_schema || '.pair_' || method_blocking_variables[i] || '_sample a' ||
                  ' JOIN ' || job_schema || '.merged_source b' ||
                  ' ON a.left_uid = b.uid AND a.blocking_variable_value = b.' || method_blocking_variables[i] ||
                  ' ORDER BY a.id';
        ELSE
            sql = 'CREATE VIEW ' || job_schema || '.method_' || i::text || '_left_sample AS' ||
                  ' SELECT ';
--                  i::text || ' AS method_index,' ||
--                  ' a.left_uid as uid,' ||
--                  ' '''' blocking_variable_value';
            FOR j IN 1 .. array_length(linkage_variables, 1) LOOP
                IF j > 1 THEN
                    sql = sql || ', ';
                END IF;
                sql = sql || 'b.' || linkage_variables[j];
            END LOOP;
            sql = sql || ' FROM ' || job_schema || '.pair_all_sample a' ||
                  ' JOIN ' || job_schema || '.merged_source b' ||
                  ' ON a.left_uid = b.uid';
        END IF;
        RAISE NOTICE '%', sql;
        EXECUTE sql;

        IF length(method_blocking_variables[i]) > 0 THEN
            sql = 'CREATE VIEW ' || job_schema || '.method_' || i::text || '_right_sample AS' ||
                  ' SELECT ';
--                  i::text || ' AS method_index,' ||
--                  ' a.right_uid as uid,' ||
--                  ' a.blocking_variable_value';
            FOR j IN 1 .. array_length(linkage_variables, 1) LOOP
                IF j > 1 THEN
                    sql = sql || ', ';
                END IF;
                sql = sql || 'b.' || linkage_variables[j];
            END LOOP;
            sql = sql || ' FROM ' || job_schema || '.pair_' || method_blocking_variables[i] || '_sample a' ||
                  ' JOIN ' || job_schema || '.merged_source b' ||
                  ' ON a.right_uid = b.uid AND a.blocking_variable_value = b.' || method_blocking_variables[i] ||
                  ' ORDER BY a.id';
        ELSE
            sql = 'CREATE VIEW ' || job_schema || '.method_' || i::text || '_right_sample AS' ||
                  ' SELECT ';
--                  i::text || ' AS method_index,' ||
--                  ' a.right_uid as uid,' ||
--                  ' '''' blocking_variable_value';
            FOR j IN 1 .. array_length(linkage_variables, 1) LOOP
                IF j > 1 THEN
                    sql = sql || ', ';
                END IF;
                sql = sql || 'b.' || linkage_variables[j];
            END LOOP;
            sql = sql || ' FROM ' || job_schema || '.pair_all_sample a' ||
                  ' JOIN ' || job_schema || '.merged_source b' ||
                  ' ON a.right_uid = b.uid';
        END IF;
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END LOOP;

END;
$BODY$;

ALTER FUNCTION public.create_job_schema(character varying, text[], text[], text[], text[], integer, boolean)
    OWNER TO honestbroker;

-- Create create_sample  function
DROP FUNCTION IF EXISTS create_sample(character varying, character varying, numeric, character varying);

CREATE OR REPLACE FUNCTION create_sample(
    job_schema character varying,
    blocking_variable character varying,
    sample_size numeric,
    sample_size_type character varying
)
    RETURNS void
LANGUAGE plpgsql
VOLATILE
COST 100
AS $BODY$
/*
 * Copyright (c) 2017. The Regents of the University of Colorado.
 * All rights reserved.
 */
DECLARE
    sql TEXT;
    limit_count bigint;
BEGIN
    IF sample_size_type = 'percent' THEN
        IF length(blocking_variable) > 0 THEN
            sql = 'SELECT floor(count(1)::numeric * (' || sample_size::text || ' / 100.0)) FROM ' || job_schema || '.pair_' || blocking_variable;
        ELSE
            sql = 'SELECT floor(count(1)::numeric * (' || sample_size::text || ' / 100.0)) FROM ' || job_schema || '.pair_all';
        END IF;
        EXECUTE sql INTO limit_count;
    ELSE
        limit_count = floor(sample_size);
    END IF;

    IF length(blocking_variable) > 0 THEN
        sql = 'INSERT INTO ' || job_schema || '.pair_' || blocking_variable || '_sample ' ||
              'SELECT id, left_uid, right_uid, blocking_variable_value FROM ' || job_schema || '.pair_' || blocking_variable || ' ' ||
              'ORDER BY left_uid, right_uid LIMIT ' || limit_count::text;
    ELSE
        sql = 'INSERT INTO ' || job_schema || '.pair_all_sample ' ||
              'SELECT id, left_uid, right_uid, blocking_variable_value FROM ' || job_schema || '.pair_all ' ||
              'ORDER BY left_uid, right_uid LIMIT ' || limit_count::text;
    END IF;
    RAISE NOTICE '%', sql;
    EXECUTE sql;
END;
$BODY$;

ALTER FUNCTION public.create_sample(character varying, character varying, numeric, character varying)
    OWNER TO honestbroker;

-- Create generate_linkage_pairs function
DROP FUNCTION IF EXISTS generate_linkage_pairs(character varying, character varying, integer);

CREATE OR REPLACE FUNCTION generate_linkage_pairs(
    job_schema character varying,
    blocking_variable character varying,
    max_block_size bigint
)
    RETURNS void
LANGUAGE plpgsql
VOLATILE
COST 100
AS $BODY$
/*
 * Copyright (c) 2017. The Regents of the University of Colorado.
 * All rights reserved.
 */
DECLARE
    sql text;
BEGIN
    IF length(blocking_variable) > 0 THEN
        sql = 'WITH temp0 AS (SELECT uid, ' || blocking_variable || ', ' ||
              'rank() OVER (PARTITION BY ' || blocking_variable || ' ORDER BY uid DESC) as rank ' ||
              'FROM ' || job_schema || '.merged_source WHERE length(coalesce(' || blocking_variable || ', '''')) > 0), ' ||
              'temp AS (SELECT * FROM temp0 WHERE (rank <= ' || max_block_size::varchar || ' or '|| max_block_size::varchar || ' <= 0)) '
              'INSERT INTO ' || job_schema || '.pair_' || blocking_variable || ' (left_uid, right_uid, blocking_variable_value) '
              'SELECT a.uid left_uid, b.uid right_uid, a.' || blocking_variable || ' ' ||
              'FROM temp a JOIN temp b on a.uid > b.uid and a.' || blocking_variable || ' = b.' || blocking_variable;
        RAISE NOTICE '%', sql;
        EXECUTE sql;

        sql = 'CREATE INDEX idx_pair_' || blocking_variable || ' ON ' || job_schema || '.pair_' || blocking_variable ||
              ' USING btree (id)';
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    ELSE
        sql = 'INSERT INTO ' || job_schema || '.' || 'pair_all (left_uid, right_uid, blocking_variable_value) ' ||
              'SELECT a.uid left_uid, b.uid right_uid, '''' ' ||
              'FROM ' || job_schema || '.merged_source a ' ||
              'JOIN ' || job_schema || '.merged_source b on a.uid > b.uid '||
              'WHERE false ';
        RAISE NOTICE '%', sql;
        EXECUTE sql;

        sql = 'CREATE INDEX idx_pair_all ON ' || job_schema || '.pair_all ' ||
              'USING btree (id)';
        RAISE NOTICE '%', sql;
        EXECUTE sql;
    END IF;
END;
$BODY$;

ALTER FUNCTION generate_linkage_pairs(character varying, character varying, bigint)
    OWNER TO honestbroker;

-- Create generate_network_ids function
-- Function: public.generate_network_ids(character varying, character varying)

-- DROP FUNCTION public.generate_network_ids(character varying, character varying);

CREATE OR REPLACE FUNCTION public.generate_network_ids(
    job_schema character varying,
    linkage_identifier character varying)
  RETURNS void AS
$BODY$
/*
 * Copyright (c) 2017. The Regents of the University of Colorado.
 * All rights reserved.
 */
DECLARE
    sql text;
BEGIN
    sql = 'DROP TABLE IF EXISTS tmp_result';
    EXECUTE sql;

    sql = 'CREATE TEMP TABLE tmp_result AS ' ||
        'SELECT DISTINCT left_uid, right_uid FROM (' ||
        'SELECT left_uid, right_uid FROM ' || job_schema || '.result WHERE match_type = ''MATCH'' ' ||
        'UNION SELECT right_uid, left_uid FROM ' || job_schema || '.result  WHERE match_type = ''MATCH'' ORDER BY 1, 2) a ' ||
        'WHERE left_uid < right_uid';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'DELETE from tmp_result a ' ||
'USING tmp_result b1, tmp_result b2 ' ||
'WHERE b1.right_uid = a.left_uid AND b2.right_uid = a.right_uid '
'AND b1.left_uid = b2.left_uid';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'DROP TABLE IF EXISTS tmp_tree';
    EXECUTE sql;

    sql = 'CREATE TEMP TABLE tmp_tree AS WITH RECURSIVE tree (root, branch) AS (' ||
    'SELECT left_uid, right_uid FROM tmp_result ' ||
        'UNION ALL SELECT a.root, b.right_uid FROM tree a JOIN tmp_result b ON a.branch = b.left_uid) ' ||
        'SELECT rank() OVER (ORDER BY root, branch) AS id, root, branch FROM tree GROUP BY root, branch ORDER BY 1, 2, 3';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'DROP TABLE IF EXISTS tmp_branch';
    EXECUTE sql;

    sql = 'CREATE TEMP TABLE tmp_branch AS WITH branches (id, root, branch) as (' ||
        'SELECT id, root, branch FROM tmp_tree ' ||
        'WHERE branch IN (SELECT branch FROM tmp_tree GROUP BY branch HAVING count(1) > 1)) ' ||
        'SELECT a.id, a.root, a.branch FROM branches a ' ||
        'LEFT OUTER JOIN (SELECT min(id) AS id FROM branches GROUP BY branch) b ON b.id = a.id ' ||
        'WHERE b.id is null';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'UPDATE tmp_tree SET root=branch, branch=root ' ||
        'WHERE id IN (SELECT id FROM tmp_branch);';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'DROP TABLE IF EXISTS tmp_tree2';
    RAISE NOTICE '%', sql;
    EXECUTE sql;
    
    sql = 'CREATE TEMP TABLE tmp_tree2 AS WITH RECURSIVE tree (root, branch) AS '||
 '(SELECT root, branch FROM tmp_tree UNION ALL SELECT a.root, b.branch FROM tree a JOIN tmp_tree b ON a.branch = b.root) '||
 'SELECT DISTINCT root, branch, case when root<=branch then root||''-''||branch else branch||''-''||root end as combine FROM tree ORDER BY 1, 2';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'DROP TABLE IF EXISTS tmp_root';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'CREATE TEMP TABLE tmp_root AS ' ||
        'SELECT distinct a.root, a.branch ' ||
        'FROM tmp_tree2 a LEFT OUTER JOIN tmp_tree2 b ON b.branch = a.root ' ||
        'WHERE b.root IS null ORDER BY 1';
    RAISE NOTICE '%', sql;
    EXECUTE sql;
    
    
    sql = 'DROP TABLE IF EXISTS temp_share';
    RAISE NOTICE '%', sql;
    EXECUTE sql;
    
sql = 'create table temp_share as '
 'select distinct a.root as root, b.root as branch, case when a.root<=b.root then a.root||''-''||b.root else b.root||''-''||a.root end as combine '||
 'from tmp_root a, tmp_root b '||
 'where a.root<> b.root and a.branch=b.branch and a.root<b.root';
RAISE NOTICE '%', sql;
    EXECUTE sql;
    
    sql= 'insert into tmp_tree2(root, branch) '||
 'select distinct a.root, a.branch '||
 'from temp_share a left join tmp_tree2 b ON a.combine = b.combine '||
 'where b.combine is null'; 
RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql= 'DELETE from tmp_tree2 a '||
 'USING tmp_tree2 b1, tmp_tree2 b2 '||
 'WHERE b1.branch = a.root AND b2.branch = a.branch '||
 'AND b1.root = b2.root'; 
RAISE NOTICE '%', sql;
    EXECUTE sql;
    
    sql = 'drop table if exists tmp_tree3';
    RAISE NOTICE '%', sql;
    EXECUTE sql;
    
    sql = 'create table tmp_tree3 as '||
  'with recursive tree (root, branch) as ('||
  'select root, branch from tmp_tree2 '||
      'union all '||
      'select a.root, b.branch from tree a '||
      'join tmp_tree2 b on a.branch = b.root ) '||
      'select distinct root, branch from tree order by 1, 2;';
RAISE NOTICE '%', sql;
    EXECUTE sql;
    
    sql = 'DROP TABLE IF EXISTS tmp_root';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'CREATE TEMP TABLE tmp_root AS ' ||
        'SELECT distinct a.root, a.branch ' ||
        'FROM tmp_tree3 a LEFT OUTER JOIN tmp_tree3 b ON b.branch = a.root ' ||
        'WHERE b.root IS null ORDER BY 1';
    RAISE NOTICE '%', sql;
    EXECUTE sql;


    sql = 'DROP TABLE IF EXISTS tmp_root_network_id';
    EXECUTE sql;

    sql = 'CREATE TEMP TABLE tmp_root_network_id AS ' ||
        'SELECT rank() OVER (ORDER BY a.source_id, a.local_id) AS network_id, a.source_id, a.local_id, a.uid AS uid ' ||
        'fROM (SELECT a.source_id, a.' || linkage_identifier || ' as local_id, b.root as uid FROM ' || job_schema || '.merged_source a ' ||
        'JOIN (SELECT DISTINCT root FROM tmp_root) b ON b.root = a.uid ' ||
        'UNION SELECT source_id, ' || linkage_identifier || ', uid FROM ' || job_schema || '.merged_source ' ||
        'WHERE uid NOT IN (SELECT DISTINCT root AS uid FROM tmp_root UNION SELECT DISTINCT branch FROM tmp_root)) a';
    RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql = 'TRUNCATE TABLE ' || job_schema || '.network_id';
    EXECUTE sql;

    sql = 'INSERT INTO ' || job_schema || '.network_id ' ||
        'SELECT a.source_id, a.local_id, network_id FROM tmp_root_network_id a ' ||
        'UNION SELECT a.source_id, a.' || linkage_identifier || ', c.network_id FROM ' || job_schema || '.merged_source a ' ||
        'JOIN tmp_tree2 b ON b.branch = a.uid JOIN tmp_root_network_id c ON c.uid = b.root ' ||
        'ORDER BY 1, 2, 3;';
    RAISE NOTICE '%', sql;
    EXECUTE sql;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION public.generate_network_ids(character varying, character varying)
  OWNER TO honestbroker;-- Create perform_deterministic_linkage function
DROP FUNCTION IF EXISTS perform_deterministic_linkage(character varying, integer, character varying, text[]);

CREATE OR REPLACE FUNCTION perform_deterministic_linkage(
    job_schema character varying,
    method_index integer,
    blocking_variable_name character varying,
    linkage_variables text[]
)
    RETURNS void
LANGUAGE plpgsql
VOLATILE
COST 100
AS $BODY$
/*
 * Copyright (c) 2017. The Regents of the University of Colorado.
 * All rights reserved.
 */
DECLARE
    sql text;
BEGIN
    sql = 'INSERT INTO ' || job_schema || '.result ' ||
          'SELECT ' ||
          method_index || ' method_index, a.left_uid, a.right_uid, ''' || blocking_variable_name || ''' blocking_variable_name, ' ||
          'a.blocking_variable_value, 100.0 confidence, ''MATCH'' match_type ' ||
          'FROM ' ||
          job_schema || '.method_' || method_index || ' a ' ||
          'WHERE';
    RAISE NOTICE '%', sql;
    FOR i IN 1 .. array_length(linkage_variables, 1) LOOP
        IF i > 1 THEN
            sql = sql || ' AND ';
        END IF;
        sql = sql || ' nullif(a.left_' || linkage_variables[i] || ', '''') = nullif(a.right_' || linkage_variables[i] || ', '''')';
    END LOOP;
    RAISE NOTICE '%', sql;
    EXECUTE sql;
END;
$BODY$;

ALTER FUNCTION public.perform_deterministic_linkage(character varying, integer, character varying, text[])
    OWNER TO honestbroker;


-- Create profile_block function
DROP FUNCTION IF EXISTS profile_block(character varying, character varying);

CREATE OR REPLACE FUNCTION profile_block(
	job_schema character varying,
	blocking_variable character varying
)
	RETURNS void
LANGUAGE plpgsql
VOLATILE
COST 100
AS $BODY$
/*
 * Copyright (c) 2017. The Regents of the University of Colorado.
 * All rights reserved.
 */
DECLARE
	sql TEXT;
BEGIN
	IF length(blocking_variable) > 0 THEN
		sql = 'INSERT INTO ' || job_schema || '.block_profile ' ||
			  'SELECT a.blocking_variable_name, count(1) block_count, avg(a.block_count) average, ' ||
			  'min(a.block_count) minimum, max(a.block_count) maximum, stddev(a.block_count) standard_deviation ' ||
			  'FROM (SELECT ''' || blocking_variable || '''::text blocking_variable_name, count(1) block_count ' ||
			  'FROM ' || job_schema || '.pair_' || blocking_variable || ' ' ||
			  'GROUP BY blocking_variable_value) a ' ||
			  'GROUP BY blocking_variable_name';
		RAISE NOTICE '%', sql;
		EXECUTE sql;
	END IF;
END;
$BODY$;

ALTER FUNCTION public.profile_block(character varying, character varying)
	OWNER TO honestbroker;

-- Create profile_result function
DROP FUNCTION IF EXISTS profile_result(character varying);

CREATE OR REPLACE FUNCTION profile_result(
	job_schema character varying
)
	RETURNS void
LANGUAGE plpgsql VOLATILE
COST 100
AS $BODY$
/*
 * Copyright (c) 2017. The Regents of the University of Colorado.
 * All rights reserved.
 */
DECLARE
	sql TEXT;
BEGIN
	sql = 'INSERT INTO ' || job_schema || '.result_profile ' ||
		  'SELECT ' ||
		  'a.method_index , '
		  'count(1) total, ' ||
		  'sum(CASE WHEN match_type = ''MATCH'' THEN 1 ELSE 0 END) matching, ' ||
		  'sum(CASE WHEN match_type = ''PROBABLE_MATCH'' THEN 1 ELSE 0 END) probable_matching, ' ||
		  'sum(CASE WHEN confidence = 100 THEN 1 ELSE 0 END) eq_100, ' ||
		  'sum(CASE WHEN confidence >= 95 AND confidence < 100 THEN 1 ELSE 0 END) ge_95_lt_100, ' ||
		  'sum(CASE WHEN confidence >= 90 AND confidence < 95 THEN 1 ELSE 0 END) ge_90_lt_95, ' ||
		  'sum(CASE WHEN confidence >= 85 AND confidence < 90 THEN 1 ELSE 0 END) ge_85_lt_90, ' ||
		  'sum(CASE WHEN confidence >= 80 AND confidence < 85 THEN 1 ELSE 0 END) ge_80_lt_85, ' ||
		  'sum(CASE WHEN confidence < 80 THEN 1 ELSE 0 END) lt_80 ' ||
		  'FROM ' || job_schema || '.result a ' ||
		  'GROUP BY ' ||
		  'a.method_index';
	RAISE NOTICE '%', sql;
	EXECUTE sql;
	RETURN;
END;
$BODY$;

ALTER FUNCTION public.profile_result(character varying)
	OWNER TO honestbroker;

