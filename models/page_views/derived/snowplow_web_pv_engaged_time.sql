{{ 
  config(
    materialized='incremental',
    unique_key='page_view_id',
    cluster_by=snowplow_utils.get_cluster_by(bigquery_cols=["page_view_id"]),
    sort='page_view_id',
    dist='page_view_id'
  ) 
}}

/*
Technically this model doesn't need to reprocess the entire session. 
Could just process new pings, i.e. where collector_tstamp >= (select max(max_collector_tstamp) from {{this}}) 
Then add old and new pings together.
Doing so may solve for 'stray page pings' issue, as we no longer consider the domain_sessionid when incremental processing page pings
*/

{{ snowplow_web.snowplow_events(
      start_date=var('snowplow__start_date'),
      tstamp_field='max_collector_tstamp',
      event_names=['page_ping']
      ) 
}}

select
  ev.page_view_id,
  max(ev.collector_tstamp) as max_collector_tstamp,
  max(ev.derived_tstamp) as end_tstamp,

  -- aggregate pings:
    -- divides epoch tstamps by snowplow__heartbeat to get distinct intervals
    -- floor rounds to nearest integer - duplicates all evaluate to the same number
    -- count(distinct) counts duplicates only once
    -- adding snowplow__min_visit_length accounts for the page view event itself.

  {{ var("snowplow__heartbeat", 10) }} * (count(distinct(floor({{ snowplow_utils.to_unixtstamp('ev.derived_tstamp') }}/{{ var("snowplow__heartbeat", 10) }}))) - 1) + {{ var("snowplow__min_visit_length", 5) }} as engaged_time_in_s

from events as ev

where ev.event_name = 'page_ping'
and ev.page_view_id is not null

group by 1
