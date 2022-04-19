{{ 
  config(
    materialized='incremental',
    sort='start_tstamp',
    dist='page_view_id'
  ) 
}}


{{ snowplow_web.snowplow_events(
        start_date=var('snowplow__start_date'),
        tstamp_field='collector_tstamp',
        event_names=['page_view', 'page_ping'],
        iab_context=var('snowplow__enable_iab', false),
        ua_context=var('snowplow__enable_ua', false),
        yauaa_context=var('snowplow__enable_yauaa', false)
        ) 
}}

, scroll_prep as (

  select
    ev.page_view_id,

    max(ev.doc_width) as doc_width,
    max(ev.doc_height) as doc_height,

    max(ev.br_viewwidth) as br_viewwidth,
    max(ev.br_viewheight) as br_viewheight,

    -- coalesce replaces null with 0 (because the page view event does send an offset)
    -- greatest prevents outliers (negative offsets)
    -- least also prevents outliers (offsets greater than the docwidth or docheight)

    least(greatest(min(coalesce(ev.pp_xoffset_min, 0)), 0), max(ev.doc_width)) as hmin, -- should be zero
    least(greatest(max(coalesce(ev.pp_xoffset_max, 0)), 0), max(ev.doc_width)) as hmax,

    least(greatest(min(coalesce(ev.pp_yoffset_min, 0)), 0), max(ev.doc_height)) as vmin, -- should be zero (edge case: not zero because the pv event is missing)
    least(greatest(max(coalesce(ev.pp_yoffset_max, 0)), 0), max(ev.doc_height)) as vmax

  from events as ev

  where ev.page_view_id is not null
    and ev.doc_height > 0 -- exclude problematic (but rare) edge case
    and ev.doc_width > 0 -- exclude problematic (but rare) edge case

  group by 1

)

, scroll_depth as (

  select
    page_view_id,

    doc_width,
    doc_height,

    br_viewwidth,
    br_viewheight,

    hmin,
    hmax,
    vmin,
    vmax,

    cast(round(100*(greatest(hmin, 0)/cast(doc_width as {{ dbt_utils.type_float() }}))) as {{ dbt_utils.type_float() }}) as relative_hmin, -- brackets matter: because hmin is of type int, we need to divide before we multiply by 100 or we risk an overflow
    cast(round(100*(least(hmax + br_viewwidth, doc_width)/cast(doc_width as {{ dbt_utils.type_float() }}))) as {{ dbt_utils.type_float() }}) as relative_hmax,
    cast(round(100*(greatest(vmin, 0)/cast(doc_height as {{ dbt_utils.type_float() }}))) as {{ dbt_utils.type_float() }}) as relative_vmin,
    cast(round(100*(least(vmax + br_viewheight, doc_height)/cast(doc_height as {{ dbt_utils.type_float() }}))) as {{ dbt_utils.type_float() }}) as relative_vmax -- not zero when a user hasn't scrolled because it includes the non-zero viewheight

  from scroll_prep

)

, page_view_events as (
  select
    ev.page_view_id,
    ev.event_id,

    ev.app_id,

    -- user fields
    ev.user_id,
    ev.domain_userid,
    ev.network_userid,

    -- session fields
    ev.domain_sessionid,
    ev.domain_sessionidx,

    -- timestamp fields
    ev.dvce_created_tstamp,
    ev.collector_tstamp,
    ev.derived_tstamp,
    ev.derived_tstamp as start_tstamp,

    ev.doc_width,
    ev.doc_height,

    ev.page_title,
    ev.page_url,
    ev.page_urlscheme,
    ev.page_urlhost,
    ev.page_urlpath,
    ev.page_urlquery,
    ev.page_urlfragment,

    ev.mkt_medium,
    ev.mkt_source,
    ev.mkt_term,
    ev.mkt_content,
    ev.mkt_campaign,
    ev.mkt_clickid,
    ev.mkt_network,

    ev.page_referrer,
    ev.refr_urlscheme,
    ev.refr_urlhost,
    ev.refr_urlpath,
    ev.refr_urlquery,
    ev.refr_urlfragment,
    ev.refr_medium,
    ev.refr_source,
    ev.refr_term,

    ev.geo_country,
    ev.geo_region,
    ev.geo_region_name,
    ev.geo_city,
    ev.geo_zipcode,
    ev.geo_latitude,
    ev.geo_longitude,
    ev.geo_timezone ,

    ev.user_ipaddress,

    ev.useragent,

    ev.br_lang,
    ev.br_viewwidth,
    ev.br_viewheight,
    ev.br_colordepth,
    ev.br_renderengine,
    ev.os_timezone,

  -- iab enrichment fields: set iab variable to true to enable
  {% if var('snowplow__enable_iab', false) %}
    ev.category,
    ev.primary_impact,
    ev.reason,
    ev.spider_or_robot,
  {% else %}
    cast(null as varchar) as category,
    cast(null as varchar) as primary_impact,
    cast(null as varchar) as reason,
    cast(null as boolean) as spider_or_robot,
  {% endif %}

  -- ua parser enrichment fields: set ua_parser variable to true to enable
  {% if var('snowplow__enable_ua', false) %}
    ev.useragent_family,
    ev.useragent_major,
    ev.useragent_minor,
    ev.useragent_patch,
    ev.useragent_version,
    ev.os_family_name, --changed name to avoid clash with column in events table
    ev.os_major,
    ev.os_minor,
    ev.os_patch,
    ev.os_patch_minor,
    ev.os_version,
    ev.device_family,
  {% else %}
    cast(null as varchar) as useragent_family,
    cast(null as varchar) as useragent_major,
    cast(null as varchar) as useragent_minor,
    cast(null as varchar) as useragent_patch,
    cast(null as varchar) as useragent_version,
    cast(null as varchar) as os_family_name,
    cast(null as varchar) as os_major,
    cast(null as varchar) as os_minor,
    cast(null as varchar) as os_patch,
    cast(null as varchar) as os_patch_minor,
    cast(null as varchar) as os_version,
    cast(null as varchar) as device_family,
  {% endif %}

  -- yauaa enrichment fields: set yauaa variable to true to enable
  {% if var('snowplow__enable_yauaa', false) %}
    ev.device_class,
    ev.agent_class,
    ev.agent_name,
    ev.agent_name_version,
    ev.agent_name_version_major,
    ev.agent_version,
    ev.agent_version_major,
    ev.device_brand,
    ev.device_name,
    ev.device_version,
    ev.layout_engine_class,
    ev.layout_engine_name,
    ev.layout_engine_name_version,
    ev.layout_engine_name_version_major,
    ev.layout_engine_version,
    ev.layout_engine_version_major,
    ev.operating_system_class,
    ev.operating_system_name,
    ev.operating_system_name_version,
    ev.operating_system_version
  {% else %}
    cast(null as varchar) as device_class,
    cast(null as varchar) as agent_class,
    cast(null as varchar) as agent_name,
    cast(null as varchar) as agent_name_version,
    cast(null as varchar) as agent_name_version_major,
    cast(null as varchar) as agent_version,
    cast(null as varchar) as agent_version_major,
    cast(null as varchar) as device_brand,
    cast(null as varchar) as device_name,
    cast(null as varchar) as device_version,
    cast(null as varchar) as layout_engine_class,
    cast(null as varchar) as layout_engine_name,
    cast(null as varchar) as layout_engine_name_version,
    cast(null as varchar) as layout_engine_name_version_major,
    cast(null as varchar) as layout_engine_version,
    cast(null as varchar) as layout_engine_version_major,
    cast(null as varchar) as operating_system_class,
    cast(null as varchar) as operating_system_name,
    cast(null as varchar) as operating_system_name_version,
    cast(null as varchar) as operating_system_version
  {% endif %}

    dense_rank() over (partition by ev.page_view_id order by ev.derived_tstamp) as page_view_id_dedupe_index

  from events as ev

  where ev.event_name = 'page_view'
  and ev.page_view_id is not null

  {% if var("snowplow__ua_bot_filter", true) %}
    and ev.useragent not similar to '%(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)%'
  {% endif %}
)

-- Dedupe: Take first row of duplicate page view, unless derived_tstamp also duplicated. 
-- Remove pv entirely if both fields are dupes. Avoids 1:many join with context tables.
, dedupe as (

  select
    *,
    count(*) over(partition by page_view_id) as row_count

  from page_view_events
  where page_view_id_dedupe_index = 1 -- Keep row(s) with earliest derived_tstamp per dupe pv

)

, cleaned_page_views as (

  select
    *,
    row_number() over (partition by pv.domain_sessionid order by pv.derived_tstamp) as page_view_in_session_index --Moved to post dedupe, unlike V1 web model.

  from dedupe as pv

  where row_count = 1 -- Remove dupe page views with more than 1 row

)

select
  ev.page_view_id,
  ev.event_id,

  ev.app_id,

  -- user fields
  ev.user_id,
  ev.domain_userid,
  ev.network_userid,

  -- session fields
  ev.domain_sessionid,
  ev.domain_sessionidx,

  ev.page_view_in_session_index,
  max(ev.page_view_in_session_index) over (partition by ev.domain_sessionid) as page_views_in_session,

  -- timestamp fields
  ev.dvce_created_tstamp,
  ev.collector_tstamp,
  ev.derived_tstamp,
  ev.start_tstamp,
  coalesce(t.end_tstamp, ev.derived_tstamp) as end_tstamp, -- only page views with pings will have a row in table t
  {{ dbt_utils.current_timestamp_in_utc() }} as model_tstamp,

  coalesce(t.engaged_time_in_s, 0) as engaged_time_in_s, -- where there are no pings, engaged time is 0.
  {{ dbt_utils.datediff('ev.derived_tstamp', 'coalesce(t.end_tstamp, ev.derived_tstamp)', 'second') }} as absolute_time_in_s,

  ev.hmax as horizontal_pixels_scrolled,
  ev.vmax as vertical_pixels_scrolled,

  ev.relative_hmax as horizontal_percentage_scrolled,
  ev.relative_vmax as vertical_percentage_scrolled,

  ev.doc_width,
  ev.doc_height,

  ev.page_title,
  ev.page_url,
  ev.page_urlscheme,
  ev.page_urlhost,
  ev.page_urlpath,
  ev.page_urlquery,
  ev.page_urlfragment,

  ev.mkt_medium,
  ev.mkt_source,
  ev.mkt_term,
  ev.mkt_content,
  ev.mkt_campaign,
  ev.mkt_clickid,
  ev.mkt_network,

  ev.page_referrer,
  ev.refr_urlscheme,
  ev.refr_urlhost,
  ev.refr_urlpath,
  ev.refr_urlquery,
  ev.refr_urlfragment,
  ev.refr_medium,
  ev.refr_source,
  ev.refr_term,

  ev.geo_country,
  ev.geo_region,
  ev.geo_region_name,
  ev.geo_city,
  ev.geo_zipcode,
  ev.geo_latitude,
  ev.geo_longitude,
  ev.geo_timezone,

  ev.user_ipaddress,

  ev.useragent,

  ev.br_lang,
  ev.br_viewwidth,
  ev.br_viewheight,
  ev.br_colordepth,
  ev.br_renderengine,

  ev.os_timezone,

  ev.category,
  ev.primary_impact,
  ev.reason,
  ev.spider_or_robot,

  ev.useragent_family,
  ev.useragent_major,
  ev.useragent_minor,
  ev.useragent_patch,
  ev.useragent_version,
  ev.os_family,
  ev.os_major,
  ev.os_minor,
  ev.os_patch,
  ev.os_patch_minor,
  ev.os_version,
  ev.device_family,

  ev.device_class,
  ev.agent_class,
  ev.agent_name,
  ev.agent_name_version,
  ev.agent_name_version_major,
  ev.agent_version,
  ev.agent_version_major,
  ev.device_brand,
  ev.device_name,
  ev.device_version,
  ev.layout_engine_class,
  ev.layout_engine_name,
  ev.layout_engine_name_version,
  ev.layout_engine_name_version_major,
  ev.layout_engine_version,
  ev.layout_engine_version_major,
  ev.operating_system_class,
  ev.operating_system_name,
  ev.operating_system_name_version,
  ev.operating_system_version

from cleaned_page_views ev

left join {{ ref('snowplow_web_pv_engaged_time') }} t
on ev.page_view_id = t.page_view_id

left join scroll_depth sd
on ev.page_view_id = sd.page_view_id
