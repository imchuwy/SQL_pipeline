CREATE OR REPLACE PROCEDURE maint.load_table_from_view(view_schema varchar, view_name varchar, table_schema varchar, table_name varchar)
	LANGUAGE plpgsql
AS $$
	

DECLARE 
	temp_table_name varchar := table_name || '_temp';
	temp_src_cols varchar := table_name || '_src_cols';
	temp_trg_cols varchar := table_name || '_trg_cols';
	i$_table varchar := 'i$_'||table_name;
	c$_table varchar := 'c$_'||table_name;
	pkeys varchar;
	query varchar;
	table_exist smallint ;
	cols varchar := '';
	cond varchar := '';
	load_type varchar :='overwrite';
	lrec record;
	srcrec record;
	trgrec record;
	cntrec record;
	cnt smallint;
	view_control record;
	new_incremental_value_filter timestamp;
	where_clause varchar :='';
	tsource varchar:=view_schema||'.'||view_name;
	ttarget varchar:=table_schema||'.'||table_name;

BEGIN
	RAISE INFO 'view name = %, table name = %',view_name,table_name;
	IF view_name IS NULL OR table_name IS NULL THEN
	    RAISE EXCEPTION 'Missing either view_name or table_name parameter';
	END IF;

	execute 'select case when count(1) > 0 then 1 else 0 end as table_exist from pg_catalog.svv_tables
		where table_schema = '''||table_schema||''' and table_name = '''||table_name||''';' into table_exist;
	
	execute 'with get_last_run as (
				select
					seq,
					incremental_value_filter,
					row_number() over(partition by seq
				order by
					last_run_ts desc) as rn
				from
					maint.load_table_from_view_log ltfvl
				where
					status = ''F'')
				select
					cnfg.seq,incremental_value_filter,incremental_column_name
				from
					get_last_run
					inner join maint.load_table_from_view_config cnfg 
					on get_last_run.seq=cnfg.seq 
					and cnfg.view_schema = '''||view_schema||'''
					and cnfg.view_name = '''||view_name||'''
				where
					rn = 1;' into view_control;
	
	raise INFO 'incremental_column_name = %',view_control.incremental_column_name;
	
	EXECUTE 'DROP TABLE IF EXISTS ' || temp_table_name||';';
	EXECUTE 'CREATE TEMP TABLE ' || temp_table_name||' (column_name varchar(256));';
	
	SELECT INTO pkeys substring(indexdef, POSITION('(' IN indexdef )+1,POSITION(')' IN indexdef )-POSITION('(' IN indexdef )-1) 
	FROM pg_indexes WHERE schemaname = table_schema AND tablename = table_name;
	
	RAISE INFO 'pkeys = %', pkeys;
	
	IF pkeys IS NOT NULL then
		EXECUTE 'INSERT INTO ' || temp_table_name||'
		with sample AS (
		  select 1 as n union all
		  select 2 union all
		  select 3 union all
		  select 4 union all
		  select 5 union all
		  select 6 union all
		  select 7 union all
		  select 8 union all
		  select 9
		)
		SELECT * FROM (
		SELECT SPLIT_PART(pkeys,'','', n) AS pkeys
		FROM (SELECT '''||pkeys||''' AS pkeys),sample
		)
		WHERE len(pkeys) >0;';
		
		EXECUTE 'SELECT count(*) as cnt FROM '||temp_table_name||';' into cnt;
	
		IF cnt>0 THEN 
			load_type:='upsert';
		END IF;
	
		if view_control.incremental_column_name is not null and view_control.incremental_value_filter is not null then 
			where_clause:=' where to_timestamp('''||view_control.incremental_column_name||''',''YYYY-MM-DD HH24:MI:SS'') > to_timestamp('''||incremental_value_filter||''',''YYYY-MM-DD HH24:MI:SS'') ';
		end if;
	
	END IF;
	
	EXECUTE 'DROP TABLE IF EXISTS ' || temp_src_cols||';';
	EXECUTE 'DROP TABLE IF EXISTS ' || temp_trg_cols||';';
	EXECUTE 'CREATE TEMP TABLE ' || temp_src_cols|| '(column_name  varchar(256),data_type  varchar(256),character_maximum_length varchar(256));';
	EXECUTE 'CREATE TEMP TABLE ' || temp_trg_cols|| '(column_name  varchar(256),data_type  varchar(256),character_maximum_length varchar(256));';
	EXECUTE 'INSERT INTO '||temp_src_cols||'(column_name,data_type,character_maximum_length) SELECT column_name,data_type,character_maximum_length FROM svv_columns c WHERE table_schema='''||view_schema||''' and table_name='''||view_name||''';';
	EXECUTE 'INSERT INTO '||temp_trg_cols||'(column_name,data_type,character_maximum_length) SELECT column_name,data_type,character_maximum_length FROM svv_columns c WHERE table_schema='''||table_schema||''' and table_name='''||table_name||''';';
	RAISE INFO 'Temp Table created';
	
	EXECUTE 'select count(*) as cnt from (select * from '||temp_src_cols||' minus select * from '||temp_trg_cols||');' into cnt;
	IF cnt > 0 THEN
		load_type:='overwrite';
	END IF;	
	
	RAISE INFO 'load_type = %', load_type;

	IF load_type='overwrite' THEN	
		execute 'drop table if exists '||ttarget||';';
		EXECUTE 'create table '||ttarget||' as select  src.* from '||tsource||' src;';
		RAISE INFO 'Table Name = % created', ttarget;	
	ELSIF load_type='upsert' THEN
		EXECUTE 'DROP TABLE IF EXISTS ' || c$_table;
		EXECUTE 'DROP TABLE IF EXISTS ' || i$_table;
		EXECUTE 'select listagg(column_name,'', '') from '||temp_trg_cols||';' into cols;
		RAISE INFO 'cols = %', cols;
		query='select listagg('''||table_name||'.''||column_name||'' = '||i$_table||'.''||column_name, '' AND '') from '||temp_table_name||';';
		raise info 'query = %', query;
		EXECUTE query into cond;
		RAISE INFO 'cond = %', cond;
		EXECUTE 'CREATE TEMP TABLE '||c$_table||' as select  '||cols||' from '||tsource||where_clause||';';
		EXECUTE 'CREATE TEMP TABLE '||i$_table||' as select '||cols||' from '||c$_table||' minus select '||cols||' from '||ttarget||';';		
		EXECUTE 'DELETE FROM '||ttarget||' USING '||i$_table||' WHERE '||cond||';';
		RAISE INFO 'DELETE PROCESSED';
		EXECUTE 'INSERT INTO '||ttarget||' ('||cols||') select  '||cols||' from '||i$_table||';';
		RAISE INFO 'Table Name = % updated', ttarget;
	 	if view_control.incremental_column_name is not null then
	 		execute 'select max('||view_control.incremental_column_name||') from'||ttarget||';' into new_incremental_value_filter;
	 		update maint.c_load_table_from_view_log set incremental_value_filter=new_incremental_value_filter where seq=view_control.seq and status='A';
	 	end if;
	END IF;
EXCEPTION
  WHEN OTHERS then
  	RAISE INFO 'Error when processing view name = %, table name = %',view_name,table_name;
END;




$$
;
