
create or replace procedure stale_table_report(days_since_activity int, truncate_table boolean)
returns varchar
language sql
comment = 'This procedure will generate a table with a list of "stale tables" to investigate. That is to say, tables that have not been created, altered, accessed, or modified since before the "days_since_activity" argument of the procedure. How to use this procedure: call stale_table_report(<days_since_last_activity>, truncate_table [TRUE | FALSE]. ex. "call stale_table_report(90, TRUE);". The first argument specifies the amount of days with no activity you consider stale. The second argument tells the procedure whether or not to truncate the output table (STALE_TABLE_REPORT), or just insert the new rows from this run.'
execute as owner
as
$$
declare
-- declare variables
return_string varchar;
rows_inserted int;

begin

-- begin explicit transaction, if there is an issue, everything in the transaction will be rolled back.
begin transaction;

-- create table STALE_TABLE_REPORT if it does not exist
create table if not exists STALE_TABLE_REPORT (
	TABLE_ID NUMBER(38,0) comment 'Internal, Snowflake-generated identifier for the table. Tables that have been recreated will have the same name, but a different TABLE_ID to track the table version.',
	TABLE_CATALOG VARCHAR(255) comment 'Database that the table belongs to.',
	TABLE_SCHEMA VARCHAR(255) comment 'Schema that the table belongs to.',
	TABLE_NAME VARCHAR(255) comment 'Name of the table.',
	LAST_ACTIVITY_TS TIMESTAMP_LTZ(6) comment 'The greatest timestamp of LAST_ACCESSED_TS, LAST_MODIFIED_TS, and LAST_ALTERED_TS.',
	LAST_ACTIVITY_DAYS_AGO INT comment 'Number of days between CURRENT_TIMESTAMP and LAST_ACTIVITY_TS',
    	TABLE_STALENESS_SCORE INT comment 'Equi-width histogram in which the histogram range is :days_since_activity though 366 days. Higher score means "more stale".',
	IS_REFERENCED_BY_OBJECT BOOLEAN comment 'Whether this table is referenced by an object.',
	LAST_ACCESSED_DAYS_AGO INT comment 'Number of days between CURRENT_TIMESTAMP and LAST_ACCESSED_TS',
	LAST_MODIFIED_DAYS_AGO INT comment 'Number of days between CURRENT_TIMESTAMP and LAST_MODIFIED_TS',
	CREATED_DAYS_AGO INT comment 'Number of days between CURRENT_TIMESTAMP and CREATED_TS',
	LAST_ALTERED_DAYS_AGO INT comment 'Number of days between CURRENT_TIMESTAMP and LAST_ALTERED_TS',
	LAST_ACCESSED_QUERY_ID VARCHAR(100) comment 'An internal, system-generated identifier for the SQL statement that last accessed this table.',
	LAST_ACCESSED_SESSION_ID NUMBER(38,0) comment 'The unique identifier for the session that last accessed this table.',
	LAST_ACCESSED_USER_NAME VARCHAR(255) comment 'The user who issued the query to last access this table.',
	LAST_ACCESSED_QUERY_TYPE VARCHAR(255) comment 'DML, query, etc. of the last query to access this table.',
	LAST_ACCESSED_APPLICATION_NAME VARCHAR(2000) comment 'Last application to access this table. Parsed APPLICATION from CLIENT_ENVIRONMENT of the session. If value is null, CLIENT_APPLICATION_ID is used.',
	LAST_MODIFIED_QUERY_ID VARCHAR(100) comment 'An internal/system-generated identifier for the SQL statement that last modified this table.',
	LAST_MODIFIED_SESSION_ID NUMBER(38,0) comment 'The unique identifier for the session that last modified this table.',
	LAST_MODIFIED_USER_NAME VARCHAR(255) comment 'The user who issued the query to last modify this table.',
	LAST_MODIFIED_QUERY_TYPE VARCHAR(255) comment 'DML, query, etc. of the last query to modify this table.',
	LAST_MODIFIED_APPLICATION_NAME VARCHAR(255) comment 'Last application to modify this table. Parsed APPLICATION from CLIENT_ENVIRONMENT of the session. If value is null, CLIENT_APPLICATION_ID is used.',
	ACCESSED_COUNT_IN_LAST_YEAR INT comment 'The number of times this table has been accessed in the last year.',
	MODIFIED_COUNT_IN_LAST_YEAR INT comment 'The number of times this table has been modified in the last year.',
	ROW_COUNT INT comment 'Number of rows in the table.',
	BYTES INT comment 'Number of bytes accessed by a scan of the table.',
	ACTIVE_BYTES INT comment 'Bytes owned by (and billed to) this table that are in the active state for the table.',
	TIME_TRAVEL_BYTES INT comment 'Bytes owned by (and billed to) this table that are in the Time Travel state for the table.',
	FAILSAFE_BYTES INT comment 'Bytes owned by (and billed to) this table that are in the Fail-safe state for the table.',
	TOTAL_STORAGE_BYTES INT comment 'SUM of ACTIVE_BYTES, TIME_TRAVEL_BYTES, FAILSAFE_BYTES.',
	GB NUMBER(38,3) comment 'Number of gigabytes accessed by a scan of the table. Calculated from TABLES.BYTES.',
	ACTIVE_GB NUMBER(38,3) comment 'Gigabyte conversion of ACTIVE_BYTES.',
	TIME_TRAVEL_GB NUMBER(38,3) comment 'Gigabyte conversion of TIME_TRAVEL_BYTES.',
	FAILSAFE_GB NUMBER(38,3) comment 'Gigabyte conversion of FAILSAFE_BYTES.',
	TOTAL_STORAGE_GB NUMBER(38,3) comment 'SUM of ACTIVE_GB, TIME_TRAVEL_GB, FAILSAFE_GB.',
	RETENTION_TIME NUMBER(38,0) comment 'Number of days that historical data is retained for Time Travel.',
	AUTO_CLUSTERING_ON BOOLEAN comment 'Whether automatic clustering is enabled on the table.',
	CLUSTERING_KEY VARCHAR(1000) comment 'Column(s) and/or expression(s) that comprise the clustering key for the table.',
	TABLE_COMMENT VARCHAR(16777216) comment 'Comment for the table.',
	IS_CLONE BOOLEAN comment 'Whether this table is a clone of another. Calculated by comparing CLONE_GROUP_ID to TABLE_ID.',
	IS_TRANSIENT BOOLEAN comment 'Whether the table is transient. Transient and temporary tables have no Fail-safe period.',
	IS_TAGGED BOOLEAN comment 'Whether this table has been assigned an OBJECT TAG.',
	IS_REFERENCED_BY_CONSTRAINT BOOLEAN comment 'Whether this table is referenced by a foreign key in another table.',
	LAST_ACCESSED_TS TIMESTAMP_LTZ(6) comment 'Timestamp of the last time this table was accessed in the last 365 days',
	LAST_MODIFIED_TS TIMESTAMP_LTZ(6) comment 'Timestamp of the last time this table was modified in the last 365 days',
	CREATED_TS TIMESTAMP_LTZ(6) comment 'Date and time when the table was created.',
	LAST_ALTERED_TS TIMESTAMP_LTZ(6) comment 'Date and time when the table was last altered by a DDL or DML operation.',
    	TABLE_OWNER VARCHAR(255) comment 'Name of the role that owns the table.',
	CLONE_GROUP_ID NUMBER(38,0) comment 'Unique identifier for the oldest clone ancestor of this table. Same as ID if the table is not a clone.',
	TABLE_CATALOG_ID NUMBER(38,0) comment 'Internal, Snowflake-generated identifier of the database for the table.',
	TABLE_SCHEMA_ID NUMBER(38,0) comment 'Internal, Snowflake-generated identifier of the schema for the table.',
	REFERENCING_OBJECTS ARRAY comment 'Array of objects with information on objects (such as views) that reference this table',
	REFERENCING_OBJECT_COUNT INT comment 'Count of objects that reference this table ',
	REFERENCING_CONSTRAINTS ARRAY comment 'Array of objects with information on tables with a reference to this table',
	REFERENCING_CONSTRAINTS_COUNT INT comment 'Count of references to this table ',
	TABLE_TAGS ARRAY comment 'Array with information on TAGS associated with this table',
	REPORT_RUN_TS TIMESTAMP_LTZ(9) comment 'The date and time this set of rows were generated by the procedure',
    	DAYS_SINCE_ACTIVITY_PARAM INT comment 'Number of days from date report was run used to filter last activity by.',
    	DAYS_SINCE_ACTIVITY_PARAM_DATE DATE comment 'The date used to filter last activity by. Last activity should be less than or equal to this date.',
    	REPORT_TABLE_TRUNCATED BOOLEAN comment 'Whether or not the last report run truncated the report table.',
	REPORT_RUN_ID INT comment 'Identifies the set of rows generated by a run of the procedure. Calculated by hashing the time current_timestamp and transaction id',
    	PID INT UNIQUE comment 'Unique ID identifying a TABLE_ID within a report run. Hash of TABLE_ID and REPORT_RUN_ID.'
)
comment = 'Contains a listing "stale" tables that have not had any activity for a set period of time as of the last time the report was run. This table is created and maintained by the procedure of the same name ("stale_table_report")';

-- truncate STALE_TABLE_REPORT if truncate_table parameter was set to TRUE
if (:truncate_table) then
    truncate table STALE_TABLE_REPORT;
end if;

-- insert rows into STALE_TABLE_REPORT
insert into STALE_TABLE_REPORT
with table_ids as (
-- retrieve current base_table ids. used for downstream filters.
select 
    t.table_id
from snowflake.account_usage.tables t
where t.deleted is null
      and t.table_type = 'BASE TABLE'
      and t.last_altered <= (current_date() - :days_since_activity)
)
, last_table_access as (
-- retrieve most recent table access metadata, ts, query_id, etc
select
      row_number() over 
        ( partition by f1.value:"objectId" 
          order by ah.query_start_time desc
        ) as row_num
    , count(*) over 
        ( partition by f1.value:"objectId"
        ) as accessed_count_in_last_year
    , ah.query_id as last_accessed_query_id
    , ah.query_start_time as last_accessed_ts
    , ah.user_name as last_accessed_user_name
    , f1.value:"objectId"::int as table_id
    , f1.value:"objectName"::varchar as table_name
from snowflake.account_usage.access_history ah
     , lateral flatten(base_objects_accessed) f1
inner join table_ids ti
    on ti.table_id = f1.value:"objectId"::int
where user_name != 'WORKSHEETS_APP_USER' -- system user for snowsight, used to save worksheets to user stage
    and ah.query_start_time <= (current_date() - :days_since_activity)
    and array_size(ah.base_objects_accessed) > 0
    and f1.value:"objectDomain"::string='Table'
qualify row_num = 1
)
, last_table_modified as (
-- retrieve most recent table modified metadata: ts, query_id, etc
select
      row_number() over 
        ( partition by f1.value:"objectId" 
          order by query_start_time desc
        ) as row_num
    , count(*) over 
        ( partition by f1.value:"objectId"
        ) as modified_count_in_last_year
    , ah.query_id as last_modified_query_id
    , ah.query_start_time as last_modified_ts
    , ah.user_name as last_modified_user_name
    , f1.value:"objectId"::int as table_id
    , f1.value:"objectName"::varchar as table_name
from snowflake.account_usage.access_history ah
     , lateral flatten(objects_modified) f1
inner join table_ids ti
    on ti.table_id = f1.value:"objectId"::int
where user_name != 'WORKSHEETS_APP_USER' -- system user for snowsight, used to save worksheets to user stage
    and query_start_time <= (current_date() - :days_since_activity)
    and array_size(objects_modified) > 0
    and f1.value:"objectDomain"::string='Table'
qualify row_num = 1
)
, query_info as (
-- retrieve query_id metadata. statement type, parsed client app, session_id
select 
      s.session_id
    , qh.query_id
    , qh.query_type
    , regexp_replace(
        coalesce(parse_json(s.client_environment):"APPLICATION"::varchar(100)
            , s.client_application_id)
    , '[1234567890]+\.*', '') as client_application_name -- remove version numbers to be tidy
from snowflake.account_usage.sessions s
    inner join snowflake.account_usage.query_history qh
        on s.session_id = qh.session_id
where qh.user_name != 'WORKSHEETS_APP_USER' -- system user for snowsight, used to save worksheets to user stage
     and qh.query_id in (
        select last_modified_query_id from last_table_modified
        union
        select last_accessed_query_id from last_table_access
     )
)
, object_dependencies_cte as (
-- construct json with array of objects for object referencing table
select 
      od.referenced_object_id
    , array_agg(
            object_construct(          
                     'dependency_type', od.dependency_type
                   , 'referencing_database', od.referencing_database
                   , 'referencing_schema', od.referencing_schema
                   , 'referencing_object_id', od.referencing_object_id
                   , 'referencing_object_name', od.referencing_object_name
                   , 'referencing_object_domain', od.referencing_object_domain
               ) 
        ) over (partition by od.referenced_object_id) as referencing_objects
from snowflake.account_usage.object_dependencies od
inner join table_ids ti
    on ti.table_id = od.referenced_object_id
where od.referenced_object_domain = 'TABLE'
)
, object_dependencies_cte_agg as (
select 
-- aggregate by object id, create count of referencing objects
      referenced_object_id
    , count(*) as referencing_object_count
    , referencing_objects
from object_dependencies_cte 
group by 1, 3)
, table_tags as (
select distinct
      tr.object_id
    , array_agg(distinct 
        object_construct(
          'tag_database', tr.tag_database
        , 'tag_schema', tr.tag_schema
        , 'tag_id', tr.tag_id
        , 'tag_name', tr.tag_name
        , 'tag_value', tr.tag_value
        ) 
      ) over (partition by tr.object_id) as table_tags
from snowflake.account_usage.tag_references tr
inner join table_ids ti
    on ti.table_id = tr.object_id
where object_deleted is null
    and domain = 'TABLE'
)
, referencing_constraints as (
-- find referencing constraints (foreign keys) that reference a unique key on the table and contruct and object
select 
      c2.table_id
    , c2.table_name
    , array_agg(
            object_construct(
                  'referencing_constraint_name', c1.constraint_catalog ||'.'||c1.constraint_schema||'.'||c1.constraint_name
                , 'referencing_constraint_id', c1.constraint_id
                , 'referencing_constraint_type', c1.constraint_type
                , 'referencing_constraint_table_id', c1.table_id
                , 'referencing_constraint_table_name', c1.table_catalog||'.'||c1.table_schema||'.'||c1.table_name
                , 'referenced_constraint_id', c2.constraint_id
                , 'referenced_constraint_name', c2.constraint_catalog ||'.'||c2.constraint_schema||'.'||c2.constraint_name
                , 'referenced_constraint_type', c2.constraint_type
                , 'referenced_constraint_table_id', c2.table_id
                , 'referenced_constraint_table_name', c2.table_catalog||'.'||c2.table_schema||'.'||c2.table_name
            ) 
    ) over (partition by c2.table_id) as referencing_constraints
from snowflake.account_usage.referential_constraints rc
inner join snowflake.account_usage.table_constraints c1
     on (rc.constraint_catalog_id, rc.constraint_schema_id, rc.constraint_name) = 
        (c1.constraint_catalog_id, c1.constraint_schema_id, c1.constraint_name)
inner join snowflake.account_usage.table_constraints c2
     on (rc.unique_constraint_catalog_id, rc.unique_constraint_schema_id, rc.unique_constraint_name) = 
        (c2.constraint_catalog_id, c2.constraint_schema_id, c2.constraint_name)
inner join table_ids ti
    on ti.table_id = c2.table_id
where rc.deleted is null
   and c1.deleted is null
   and c2.deleted is null
)
, referencing_constraints_agg as (
-- aggregate referencing constraint objects by table_id  
select
      rc.table_id
    , rc.referencing_constraints
    , count(*) as referencing_constraints_count
from referencing_constraints rc
group by rc.table_id
       , rc.referencing_constraints
)
-- compose final result using active base tables as anchor
select
      t.table_id
    , t.table_catalog
    , t.table_schema
    , t.table_name
    , greatest(
              nvl(t.last_altered, 0::timestamp_ltz)
            , nvl(lta.last_accessed_ts, 0::timestamp_ltz)
            , nvl(ltm.last_modified_ts, 0::timestamp_ltz)
      ) as last_activity_ts
    , datediff(days, last_activity_ts, current_timestamp()) as last_activity_days_ago
    , width_bucket(last_activity_days_ago, :days_since_activity, 366, 6) as table_staleness_score
    , iff(oda.referencing_objects is null, FALSE, TRUE) as is_referenced_by_object
    , datediff(days, lta.last_accessed_ts, current_timestamp()) as last_accessed_days_ago
    , datediff(days, ltm.last_modified_ts, current_timestamp()) as last_modified_days_ago
    , datediff(days, t.created, current_timestamp()) as created_days_ago
    , datediff(days, t.last_altered, current_timestamp()) as last_altered_days_ago
    , lta.last_accessed_query_id
    , lta_qi.session_id as last_accessed_session_id
    , lta.last_accessed_user_name
    , lta_qi.query_type as last_accessed_query_type
    , lta_qi.client_application_name as last_accessed_application_name
    , ltm.last_modified_query_id
    , ltm_qi.session_id as last_modified_session_id
    , ltm.last_modified_user_name
    , ltm_qi.query_type as last_modified_query_type
    , ltm_qi.client_application_name as last_modified_application_name
    , ifnull(lta.accessed_count_in_last_year, 0) as accessed_count_in_last_year
    , ifnull(ltm.modified_count_in_last_year, 0) as modified_count_in_last_year
    , t.row_count
    , t.bytes 
    , tsm.active_bytes
    , tsm.time_travel_bytes
    , tsm.failsafe_bytes
    , (tsm.active_bytes + tsm.time_travel_bytes + tsm.failsafe_bytes) as total_storage_bytes
    , round(t.bytes/pow(1024,3),3) as gb
    , round(tsm.active_bytes/pow(1024,3),3) as active_gb
    , round(tsm.time_travel_bytes/pow(1024,3),3) as time_travel_gb
    , round(tsm.failsafe_bytes/pow(1024,3),3) as failsafe_gb
    , (active_gb + time_travel_gb + failsafe_gb) as total_storage_gb
    , t.retention_time
    , iff(t.auto_clustering_on = 'YES', TRUE, FALSE) as auto_clustering_on
    , t.clustering_key
    , t."COMMENT" as table_comment
    , iff(t.table_id != tsm.clone_group_id, TRUE, FALSE) as is_clone
    , iff(t.is_transient = 'YES', TRUE, FALSE) as is_transient
    , iff(tt.table_tags is null, FALSE, TRUE) as is_tagged
    , iff(rca.referencing_constraints is null, FALSE, TRUE) as is_referenced_by_constraint
    , lta.last_accessed_ts
    , ltm.last_modified_ts
    , t.created as created_ts
    , t.last_altered as last_altered_ts
    , t.table_owner
    , tsm.clone_group_id
    , t.table_catalog_id
    , t.table_schema_id
    , oda.referencing_objects
    , ifnull(oda.referencing_object_count, 0) as referencing_object_count
    , rca.referencing_constraints
    , ifnull(rca.referencing_constraints_count, 0) as referencing_constraints_count
    , tt.table_tags
    , current_timestamp() as report_run_ts
    , :days_since_activity as days_since_activity_param
    , report_run_ts::date - :days_since_activity as days_since_activity_param_date
    , :truncate_table as report_table_truncated
    , hash(report_run_ts, current_transaction()) as report_run_id
    , hash(report_run_id, t.table_id) as pid
from snowflake.account_usage.tables t
inner join table_ids ti
    on ti.table_id = t.table_id
left outer join snowflake.account_usage.table_storage_metrics tsm
    on t.table_id = tsm.id
left outer join last_table_access lta
    on t.table_id = lta.table_id
left outer join last_table_modified ltm
    on t.table_id = ltm.table_id
left outer join query_info ltm_qi
    on ltm_qi.query_id = ltm.last_modified_query_id
left outer join query_info lta_qi
    on lta_qi.query_id = lta.last_accessed_query_id
left outer join object_dependencies_cte_agg as oda
    on t.table_id = oda.referenced_object_id
left outer join table_tags tt
    on t.table_id = tt.object_id
left outer join referencing_constraints_agg rca
    on t.table_id = rca.table_id
where t.deleted is null
order by t.table_catalog
        ,t.table_schema
        ,t.table_name;

-- capture the number of rows inserted into the table
rows_inserted := sqlrowcount;

-- construct string and assign the result to the return_string variable
select 'STALE_TABLE_REPORT table was '||iff(:truncate_table,'truncated', 'not truncated') || ' and ' || :rows_inserted::varchar || ' stale tables were found and inserted into ' || current_database()||'.'||current_schema()||'.'||'STALE_TABLE_REPORT' into :return_string;

-- commit transaction
commit;

-- return the value of return_string as the result
return return_string;

exception
  when statement_error then
    return object_construct('Error type', 'STATEMENT_ERROR',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);
  when other then
    return object_construct('Error type', 'Other error',
                            'SQLCODE', sqlcode,
                            'SQLERRM', sqlerrm,
                            'SQLSTATE', sqlstate);

end;
$$;






