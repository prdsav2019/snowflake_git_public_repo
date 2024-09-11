# Import python packages
import streamlit as st
import plotly.express as px
from snowflake.snowpark.context import get_active_session


# Write directly to the app
st.title("Stale Table Report :Snowflake")
st.write(
    """
    This Report provides information on Stale Tables within the Account """
)

st.info("Note: This data is sourced from the Snowflake Account Usage Schema, which retains data for one year. To extend analysis beyond this period, consider setting up periodic backups of the Account Usage views.")
st.divider()



# Get the current credentials
session = get_active_session()


tbl_cnt_schema_query="""
select TABLE_SCHEMA, COUNT(TABLE_NAME) as TABLE_COUNT
from util_db.ADMIN_TOOLS.STALE_TABLE_REPORT
where TABLE_SCHEMA is not null
group by TABLE_SCHEMA ORDER BY 2 DESC;
"""

st.subheader("Count of Stale Tables by Schema")
with st.spinner('Query Running...'):
    tbl_cnt_schema_df=session.sql(tbl_cnt_schema_query).collect()
    st.bar_chart(data=tbl_cnt_schema_df,x="TABLE_SCHEMA",y="TABLE_COUNT",use_container_width=True)




tbl_cnt_db_query="""
select TABLE_CATALOG,TABLE_SCHEMA, COUNT(TABLE_NAME) as TABLE_COUNT
from util_db.ADMIN_TOOLS.STALE_TABLE_REPORT
where TABLE_SCHEMA is not null and TABLE_CATALOG is not null
group by TABLE_CATALOG,TABLE_SCHEMA ORDER BY 3 DESC
Limit 10;
"""

st.subheader("Count of Stale Tables by Database and Schema")
with st.spinner('Query Running...'):
    tbl_cnt_db_df=session.sql(tbl_cnt_db_query).collect()
    fig = px.scatter_3d(tbl_cnt_db_df, x='TABLE_CATALOG', y='TABLE_SCHEMA', z='TABLE_COUNT',
                    color='TABLE_COUNT', size='TABLE_COUNT',
                    title=' Plot of top 10 Table Counts by Database and Schema')
    st.plotly_chart(fig, use_container_width=True)




stale_tbl_query="""
SELECT TABLE_CATALOG as DATABASE,TABLE_SCHEMA,TABLE_NAME, LAST_ACTIVITY_DAYS_AGO
FROM util_db.ADMIN_TOOLS.STALE_TABLE_REPORT
ORDER BY LAST_ACTIVITY_DAYS_AGO DESC;
"""
st.subheader("List of Tables by Inactivity Period")

with st.spinner('Query Running...'):
    top_stale_tbl_df=session.sql(stale_tbl_query).collect()
    st.dataframe(top_stale_tbl_df,hide_index=True)


st.subheader("Total Storage Usage by Schema for Stale Tables")
stale_tables_storage_query="""
select TABLE_SCHEMA,SUM(TOTAL_STORAGE_GB) as TOTAL_STORAGE_GB
from util_db.ADMIN_TOOLS.STALE_TABLE_REPORT
where TABLE_SCHEMA is not null 
GROUP BY 1 ORDER BY 2 DESC;
"""
with st.spinner('Query Running...'):
     stale_tables_storage_df=session.sql(stale_tables_storage_query).collect()
     st.dataframe(stale_tables_storage_df,hide_index=True)




last_access_user_name_query="""
select LAST_ACCESSED_USER_NAME,LAST_ACCESSED_APPLICATION_NAME,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME ,LAST_ACCESSED_DAYS_AGO
from util_db.ADMIN_TOOLS.STALE_TABLE_REPORT
where LAST_ACCESSED_USER_NAME is not null
order by LAST_ACCESSED_DAYS_AGO desc;
"""
st.subheader('Recent Table Access Details by User and Application')
with st.spinner('Query Running...'):
     last_access_user_name_df=session.sql(last_access_user_name_query).collect()
     st.dataframe(last_access_user_name_df,hide_index=True)


st.divider()

stale_schema_query="""
WITH object_access_hist AS (
    SELECT
        AH.QUERY_ID,
        AH.QUERY_START_TIME,
        AH.USER_NAME,
        DO_ACC_L1.VALUE:objectName::STRING AS OBJECT_NAME,
        DO_ACC_L1.VALUE:objectDomain::STRING AS OBJECT_TYPE,
        DO_ACC_L1.VALUE:objectId::NUMBER AS OBJECT_ID,
        SPLIT_PART(OBJECT_NAME, '.', 1) AS TABLE_CATALOG,
        SPLIT_PART(OBJECT_NAME, '.', 2) AS TABLE_SCHEMA,
        SPLIT_PART(OBJECT_NAME, '.', 3) AS TABLE_NAME,
        DO_ACC_L1.VALUE:columns AS TABLE_COLUMNS,
        ARRAY_SIZE(DO_ACC_L1.VALUE:columns) AS NUM_TABLE_COLUMNS
    FROM 
        SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY AH,
        LATERAL FLATTEN(INPUT => AH.DIRECT_OBJECTS_ACCESSED) DO_ACC_L1
    WHERE 
        AH.QUERY_START_TIME >= DATEADD(month, -6, CURRENT_TIMESTAMP)
)
SELECT 
    catalog_name AS db_name,
    schema_name,
    created,
    last_altered,
    schema_owner 
FROM 
    SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA 
WHERE  
    deleted IS NULL 
    AND schema_name != 'PUBLIC'  
    AND CONCAT(catalog_name, '.', schema_name) NOT IN (
        SELECT DISTINCT CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA) 
        FROM object_access_hist where TABLE_CATALOG!='SNOWFLAKE'
    ) and last_altered < DATEADD(month, -12, CURRENT_TIMESTAMP) order by last_altered asc;
"""

st.subheader('Stale Schemas')
st.info("Note: Schemas in your Snowflake account that have not been accessed in the last six months and have not been altered in the last 12 months (DML or DDL) ")

with st.spinner('Query Running...'):
     stale_schema_df=session.sql(stale_schema_query).collect()
     st.dataframe(stale_schema_df,hide_index=True)


