{{
  config(
    partition_by = snowplow_utils.get_partition_by(bigquery_partition_by={
      "field": "start_tstamp",
      "data_type": "timestamp"
    }),
    cluster_by=snowplow_utils.get_cluster_by(bigquery_cols=["domain_userid"]),
    sort='domain_userid',
    dist='domain_userid',
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with prep as (
  select
    domain_userid,
    -- time
    user_start_tstamp as start_tstamp,
    user_end_tstamp as end_tstamp,
    user_first_sessionid as first_sessionid,
    user_last_sessionid as last_sessionid,
    user_first_page_view_id as first_page_view_id,
    user_first_page_view_event_id as first_page_view_event_id,
    user_last_page_view_id as last_page_view_id,
    user_last_page_view_event_id as last_page_view_event_id,
    -- first/last session. Max to resolve edge case with multiple sessions with the same start/end tstamp
    max(case when start_tstamp = user_start_tstamp then domain_sessionid end) as first_domain_sessionid,
    max(case when end_tstamp = user_end_tstamp then domain_sessionid end) as last_domain_sessionid,
    -- {{ snowplow_web.bool_or("converted_session") }} as converted_user,
    max(converted_date) as converted_date,
    -- engagement
    sum(page_views) as page_views,
    count(distinct domain_sessionid) as sessions,
    sum(engaged_time_in_s) as engaged_time_in_s

  from {{ ref('snowplow_web_users_sessions_this_run') }}

  group by 1,2,3,4,5,6,7,8,9
)

select
  domain_userid,
  start_tstamp,
  end_tstamp,
  first_sessionid,
  last_sessionid,
  first_page_view_id,
  first_page_view_event_id,
  last_page_view_id,
  last_page_view_event_id,
  first_domain_sessionid,
  last_domain_sessionid,
  converted_date IS NOT NULL as converted_user,
  converted_date,
  page_views,
  sessions,
  engaged_time_in_s

from prep
