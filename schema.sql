--
-- FIXME: development only
--
drop schema public cascade;
create schema public;
grant all on schema public to postgres;
grant all on schema public to public;
comment on schema public is 'standard public schema';

create table log_record (
  id                  serial primary key,
  type                text not null,
  data                text not null
);

create index on log_record (type);

-------------------------------------------------------------------------------
-- STORED PROCEDURES ----------------------------------------------------------
-------------------------------------------------------------------------------

create function check_backlog(integer)
returns void language plpgsql as $$
  declare
    n integer;
  begin
    select n_live_tup into n
      from pg_stat_all_tables
      where relname = 'log_record';
    if (n > $1) then
      raise exception using
        message = 'more than ' || $1 || ' records processing',
        errcode = 'BCKLG';
    end if;
    return;
  end;
$$;

create function get_log_records(text, integer)
returns setof log_record language plpgsql as $$
  begin
    return query select * from log_record
    where type = $1 limit ($2) for update skip locked;
  end
$$;
