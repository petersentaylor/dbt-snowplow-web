{{
  config(
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

select
  domain_sessionid,
  -- time
  min(start_tstamp) as start_tstamp,
  max(end_tstamp) as end_tstamp,

  {{ snowplow_web.bool_or("converted_pageview") }} as converted_session,
  max(converted_date) as converted_date,

  -- engagement
  count(distinct page_view_id) as page_views,
  sum(engaged_time_in_s) as engaged_time_in_s

from {{ ref('snowplow_web_page_views_this_run') }}

where domain_sessionid is not null
group by 1
