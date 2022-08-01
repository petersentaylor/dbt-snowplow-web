{{
  config(
    tags=["this_run"],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

with user_ids_this_run as (
select distinct domain_userid from {{ ref('snowplow_web_base_sessions_this_run') }}
)

select
  a.*,
  min(a.start_tstamp) over(partition by a.domain_userid) as user_start_tstamp,
  max(a.end_tstamp) over(partition by a.domain_userid) as user_end_tstamp,
  {% if target.type == 'snowflake' %}
  first_value(a.domain_sessionid) over (partition by a.domain_userid order by start_tstamp) as user_first_sessionid,
  last_value(a.domain_sessionid) over (partition by a.domain_userid order by start_tstamp) as user_last_sessionid,
  first_value(a.first_page_view_id) over (partition by a.domain_userid order by start_tstamp) as user_first_page_view_id,
  first_value(a.first_page_view_event_id) over (partition by a.domain_userid order by start_tstamp) as user_first_page_view_event_id,
  last_value(a.last_page_view_id) over (partition by a.domain_userid order by start_tstamp) as user_last_page_view_id,
  last_value(a.last_page_view_event_id) over (partition by a.domain_userid order by start_tstamp) as user_last_page_view_event_id
  {% else %}
  first_value(a.domain_sessionid) over (partition by a.domain_userid order by start_tstamp ROWS UNBOUNDED PRECEDING) as user_first_sessionid,
  last_value(a.domain_sessionid) over (partition by a.domain_userid order by start_tstamp ROWS UNBOUNDED PRECEDING) as user_last_sessionid,
  first_value(a.first_page_view_id) over (partition by a.domain_userid order by start_tstamp ROWS UNBOUNDED PRECEDING) as user_first_page_view_id,
  first_value(a.first_page_view_event_id) over (partition by a.domain_userid order by start_tstamp ROWS UNBOUNDED PRECEDING) as user_first_page_view_event_id,
  last_value(a.last_page_view_id) over (partition by a.domain_userid order by start_tstamp ROWS UNBOUNDED PRECEDING) as user_last_page_view_id,
  last_value(a.last_page_view_event_id) over (partition by a.domain_userid order by start_tstamp ROWS UNBOUNDED PRECEDING) as user_last_page_view_event_id

  {% endif %}

from {{ var('snowplow__sessions_table') }} a
inner join user_ids_this_run b
on a.domain_userid = b.domain_userid
