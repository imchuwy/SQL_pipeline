CREATE OR REPLACE PROCEDURE maint.load_table_from_view_wrapper()
	LANGUAGE plpgsql
AS $$
	
declare 
	wraprec record;
	cnt smallint;
	pview_seq bigint;
	pview_schema varchar;
	pview_name varchar;
	max_time timestamp;
	plast_run_ts timestamp;
begin
	execute 'select count(1) from maint.load_table_from_view_log where status = ''A'';' into cnt;
	if cnt = 0 then
		for wraprec in select * from maint.load_table_from_view_config order by seq loop
			raise info 'Test';
			pview_seq := wraprec.seq ;
			pview_schema := wraprec.view_schema;
			pview_name := wraprec.view_name;
			raise info 'pview_seq = %, pview_schema = %, pview_name = %',pview_seq,pview_schema,pview_name;
			insert into maint.load_table_from_view_log (seq,incremental_value_filter,status,last_run_ts) values (pview_seq,max_time,'A',null );
			call maint.load_table_from_view(wraprec.view_schema,wraprec.view_name,wraprec.table_schema,wraprec.table_name);
			plast_run_ts := current_timestamp;
			update maint.load_table_from_view_log set status='F', last_run_ts = plast_run_ts where seq=pview_seq and status = 'A';
			update maint.load_table_from_view_config set last_run_ts = plast_run_ts where seq=pview_seq;			
		end loop;
	end if;
EXCEPTION
  WHEN OTHERS then
  	plast_run_ts := current_timestamp;
  	update maint.load_table_from_view_log set status='E', last_run_ts = plast_run_ts where seq=pview_seq and status = 'A';
	update maint.load_table_from_view_config set last_run_ts = plast_run_ts where seq=pview_seq;
end;
$$
;
