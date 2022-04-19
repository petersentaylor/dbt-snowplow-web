{{ 
  config(
    materialized='incremental',
    unique_key='session_id',
    upsert_date_key='min_collector_tstamp',
    sort='min_collector_tstamp',
    dist='session_id',
    partition_by = {
      "field": "min_collector_tstamp",
      "data_type": "timestamp"
    },
    tags=["manifest"],
    enabled=true
  ) 
}}

-- Known edge cases:
-- 1: Rare case with multiple domain_userid per session.

with new_events_session_ids as (
  select
    e.domain_sessionid as session_id,
    max(e.domain_userid) as domain_userid, -- Edge case 1: Arbitary selection to avoid window function like first_value.
    min(e.collector_tstamp) as min_collector_tstamp,
    max(e.collector_tstamp) as max_collector_tstamp

  from {{ var('snowplow__events') }} e

  where
    e.domain_sessionid is not null
    and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}
    and e.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'dvce_created_tstamp') }} -- don't process data that's too late
    and e.collector_tstamp >= 
    {% if is_incremental() %}
      {{ snowplow_utils.timestamp_add('hour',var("snowplow__lookback_window_hours") , snowplow_web.get_start_ts(this, field='min_collector_tstamp')) }}
    {% else %}
      '{{ var("snowplow__start_date") }}'
    {% endif %}
    {% if var('snowplow__derived_tstamp_partitioned', true) and target.type == 'bigquery' | as_bool() %} -- BQ only
      and e.derived_tstamp >=
      {% if is_incremental() %}
        {{ snowplow_utils.timestamp_add('hour',var("snowplow__lookback_window_hours") , snowplow_web.get_start_ts(this, field='min_collector_tstamp')) }}
      {% else %}
        '{{ var("snowplow__start_date") }}'
      {% endif %}
    {% endif %}

  group by 1
)

{% if is_incremental() %} 

, previous_sessions as (
  select *

  from {{ this }}

  where min_collector_tstamp >= {{ snowplow_utils.timestamp_add('hour',-var("snowplow__session_lookback_days") , snowplow_web.get_start_ts(this, field='min_collector_tstamp')) }}
)

, session_lifecycle as (
  select
    ns.session_id,
    coalesce(self.domain_userid, ns.domain_userid) as domain_userid, -- Edge case 1: Take previous value to keep domain_userid consistent. Not deterministic but performant
    least(ns.min_collector_tstamp, coalesce(self.min_collector_tstamp, ns.min_collector_tstamp)) as min_collector_tstamp,
    greatest(ns.max_collector_tstamp, coalesce(self.max_collector_tstamp, ns.max_collector_tstamp)) as max_collector_tstamp -- BQ 1 NULL will return null hence coalesce
    
  from new_events_session_ids ns
  left join previous_sessions as self
    on ns.session_id = self.session_id

  where
    self.session_id is null -- process all new sessions
    or self.max_collector_tstamp < {{ snowplow_utils.timestamp_add('day', var("snowplow__max_session_days", 3), 'self.min_collector_tstamp') }} --stop updating sessions exceeding 3 days
  )

{% else %}

, session_lifecycle as (

  select * from new_events_session_ids

)

{% endif %}

select
  sl.session_id,
  sl.domain_userid,
  sl.min_collector_tstamp,
  least({{ snowplow_utils.timestamp_add('day', var("snowplow__max_session_days", 3), 'sl.min_collector_tstamp') }}, sl.max_collector_tstamp) as max_collector_tstamp -- limit session length to max_session_days

from session_lifecycle sl

