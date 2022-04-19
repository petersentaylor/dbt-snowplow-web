{{ 
  config(
    materialized='incremental',
    unique_key='page_view_id'
  ) 
}}

-- Should add the cols arg here to minimise columns selected.

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

, pv_prep as (

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

    -- optional fields, only populated if enabled.

    -- iab enrichment fields: set iab variable to true to enable
    {% if var('snowplow__enable_iab', false) %}

    ev.category,
    ev.primary_impact,
    ev.reason,
    ev.spider_or_robot,

    {% else %}

    cast(null as {{ dbt_utils.type_string() }}) as category,
    cast(null as {{ dbt_utils.type_string() }}) as primary_impact,
    cast(null as {{ dbt_utils.type_string() }}) as reason,
    cast(null as boolean) as spider_or_robot,

    {% endif %}

    -- ua parser enrichment fields: set ua_parser variable to true to enable
    {% if var('snowplow__enable_ua', false) %}

    ev.useragent_family,
    ev.useragent_major,
    ev.useragent_minor,
    ev.useragent_patch,
    ev.useragent_version,
    ev.os_family_name,  --NOTE: Changed name to avoid clash with 'os_family' in events table
    ev.os_major,
    ev.os_minor,
    ev.os_patch,
    ev.os_patch_minor,
    ev.os_version,
    ev.device_family,

    {% else %}

    cast(null as {{ dbt_utils.type_string() }}) as useragent_family,
    cast(null as {{ dbt_utils.type_string() }}) as useragent_major,
    cast(null as {{ dbt_utils.type_string() }}) as useragent_minor,
    cast(null as {{ dbt_utils.type_string() }}) as useragent_patch,
    cast(null as {{ dbt_utils.type_string() }}) as useragent_version,
    cast(null as {{ dbt_utils.type_string() }}) as os_family_name,  --NOTE: Changed name to avoid clash with 'os_family' in events table
    cast(null as {{ dbt_utils.type_string() }}) as os_major,
    cast(null as {{ dbt_utils.type_string() }}) as os_minor,
    cast(null as {{ dbt_utils.type_string() }}) as os_patch,
    cast(null as {{ dbt_utils.type_string() }}) as os_patch_minor,
    cast(null as {{ dbt_utils.type_string() }}) as os_version,
    cast(null as {{ dbt_utils.type_string() }}) as device_family,

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

    cast(null as {{ dbt_utils.type_string() }}) as device_class,
    cast(null as {{ dbt_utils.type_string() }}) as agent_class,
    cast(null as {{ dbt_utils.type_string() }}) as agent_name,
    cast(null as {{ dbt_utils.type_string() }}) as agent_name_version,
    cast(null as {{ dbt_utils.type_string() }}) as agent_name_version_major,
    cast(null as {{ dbt_utils.type_string() }}) as agent_version,
    cast(null as {{ dbt_utils.type_string() }}) as agent_version_major,
    cast(null as {{ dbt_utils.type_string() }}) as device_brand,
    cast(null as {{ dbt_utils.type_string() }}) as device_name,
    cast(null as {{ dbt_utils.type_string() }}) as device_version,
    cast(null as {{ dbt_utils.type_string() }}) as layout_engine_class,
    cast(null as {{ dbt_utils.type_string() }}) as layout_engine_name,
    cast(null as {{ dbt_utils.type_string() }}) as layout_engine_name_version,
    cast(null as {{ dbt_utils.type_string() }}) as layout_engine_name_version_major,
    cast(null as {{ dbt_utils.type_string() }}) as layout_engine_version,
    cast(null as {{ dbt_utils.type_string() }}) as layout_engine_version_major,
    cast(null as {{ dbt_utils.type_string() }}) as operating_system_class,
    cast(null as {{ dbt_utils.type_string() }}) as operating_system_name,
    cast(null as {{ dbt_utils.type_string() }}) as operating_system_name_version,
    cast(null as {{ dbt_utils.type_string() }}) as operating_system_version

    {% endif %}

    from events as ev
    
    where ev.event_name = 'page_view'
    and ev.page_view_id is not null

    {% if var("snowplow__ua_bot_filter", true) %}
       and not rlike(ev.useragent, '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*')
    {% endif %}

    qualify row_number() over (partition by ev.page_view_id order by ev.derived_tstamp) = 1

)

, page_view_events as (

  select
    p.page_view_id,
    p.event_id,

    p.app_id,

    -- user fields
    p.user_id,
    p.domain_userid,
    p.network_userid,

    -- session fields
    p.domain_sessionid,
    p.domain_sessionidx,

    row_number() over (partition by p.domain_sessionid order by p.derived_tstamp) AS page_view_in_session_index,

    -- timestamp fields
    p.dvce_created_tstamp,
    p.collector_tstamp,
    p.derived_tstamp,
    p.start_tstamp,
    coalesce(t.end_tstamp, p.derived_tstamp) as end_tstamp, -- only page views with pings will have a row in table t
    {{ dbt_utils.current_timestamp_in_utc() }} as model_tstamp,

    coalesce(t.engaged_time_in_s, 0) as engaged_time_in_s, -- where there are no pings, engaged time is 0.
    timediff(second, p.derived_tstamp, coalesce(t.end_tstamp, p.derived_tstamp))  as absolute_time_in_s,

    sd.hmax as horizontal_pixels_scrolled,
    sd.vmax as vertical_pixels_scrolled,

    sd.relative_hmax as horizontal_percentage_scrolled,
    sd.relative_vmax as vertical_percentage_scrolled,

    p.doc_width,
    p.doc_height,

    p.page_title,
    p.page_url,
    p.page_urlscheme,
    p.page_urlhost,
    p.page_urlpath,
    p.page_urlquery,
    p.page_urlfragment,

    p.mkt_medium,
    p.mkt_source,
    p.mkt_term,
    p.mkt_content,
    p.mkt_campaign,
    p.mkt_clickid,
    p.mkt_network,

    p.page_referrer,
    p.refr_urlscheme,
    p.refr_urlhost,
    p.refr_urlpath,
    p.refr_urlquery,
    p.refr_urlfragment,
    p.refr_medium,
    p.refr_source,
    p.refr_term,

    p.geo_country,
    p.geo_region,
    p.geo_region_name,
    p.geo_city,
    p.geo_zipcode,
    p.geo_latitude,
    p.geo_longitude,
    p.geo_timezone,

    p.user_ipaddress,

    p.useragent,

    p.br_lang,
    p.br_viewwidth,
    p.br_viewheight,
    p.br_colordepth,
    p.br_renderengine,

    p.os_timezone,

    p.category,
    p.primary_impact,
    p.reason,
    p.spider_or_robot,

    p.useragent_family,
    p.useragent_major,
    p.useragent_minor,
    p.useragent_patch,
    p.useragent_version,
    p.os_family_name,
    p.os_major,
    p.os_minor,
    p.os_patch,
    p.os_patch_minor,
    p.os_version,
    p.device_family,

    p.device_class,
    p.agent_class,
    p.agent_name,
    p.agent_name_version,
    p.agent_name_version_major,
    p.agent_version,
    p.agent_version_major,
    p.device_brand,
    p.device_name,
    p.device_version,
    p.layout_engine_class,
    p.layout_engine_name,
    p.layout_engine_name_version,
    p.layout_engine_name_version_major,
    p.layout_engine_version,
    p.layout_engine_version_major,
    p.operating_system_class,
    p.operating_system_name,
    p.operating_system_name_version,
    p.operating_system_version

  from pv_prep p

  left join {{ ref('snowplow_web_pv_engaged_time') }} t
  on p.page_view_id = t.page_view_id

  left join scroll_depth sd
  on p.page_view_id = sd.page_view_id
  
)

select
  pve.page_view_id,
  pve.event_id,

  pve.app_id,

  -- user fields
  pve.user_id,
  pve.domain_userid,
  pve.network_userid,

  -- session fields
  pve.domain_sessionid,
  pve.domain_sessionidx,

  pve.page_view_in_session_index,
  max(pve.page_view_in_session_index) over (partition by pve.domain_sessionid) as page_views_in_session,

  -- timestamp fields
  pve.dvce_created_tstamp,
  pve.collector_tstamp,
  pve.derived_tstamp,
  pve.start_tstamp,
  pve.end_tstamp,
  pve.model_tstamp,

  pve.engaged_time_in_s,
  pve.absolute_time_in_s,

  pve.horizontal_pixels_scrolled,
  pve.vertical_pixels_scrolled,

  pve.horizontal_percentage_scrolled,
  pve.vertical_percentage_scrolled,

  pve.doc_width,
  pve.doc_height,

  pve.page_title,
  pve.page_url,
  pve.page_urlscheme,
  pve.page_urlhost,
  pve.page_urlpath,
  pve.page_urlquery,
  pve.page_urlfragment,

  pve.mkt_medium,
  pve.mkt_source,
  pve.mkt_term,
  pve.mkt_content,
  pve.mkt_campaign,
  pve.mkt_clickid,
  pve.mkt_network,

  pve.page_referrer,
  pve.refr_urlscheme,
  pve.refr_urlhost,
  pve.refr_urlpath,
  pve.refr_urlquery,
  pve.refr_urlfragment,
  pve.refr_medium,
  pve.refr_source,
  pve.refr_term,

  pve.geo_country,
  pve.geo_region,
  pve.geo_region_name,
  pve.geo_city,
  pve.geo_zipcode,
  pve.geo_latitude,
  pve.geo_longitude,
  pve.geo_timezone,

  pve.user_ipaddress,

  pve.useragent,

  pve.br_lang,
  pve.br_viewwidth,
  pve.br_viewheight,
  pve.br_colordepth,
  pve.br_renderengine,

  pve.os_timezone,

  pve.category,
  pve.primary_impact,
  pve.reason,
  pve.spider_or_robot,

  pve.useragent_family,
  pve.useragent_major,
  pve.useragent_minor,
  pve.useragent_patch,
  pve.useragent_version,
  pve.os_family_name,
  pve.os_major,
  pve.os_minor,
  pve.os_patch,
  pve.os_patch_minor,
  pve.os_version,
  pve.device_family,

  pve.device_class,
  pve.agent_class,
  pve.agent_name,
  pve.agent_name_version,
  pve.agent_name_version_major,
  pve.agent_version,
  pve.agent_version_major,
  pve.device_brand,
  pve.device_name,
  pve.device_version,
  pve.layout_engine_class,
  pve.layout_engine_name,
  pve.layout_engine_name_version,
  pve.layout_engine_name_version_major,
  pve.layout_engine_version,
  pve.layout_engine_version_major,
  pve.operating_system_class,
  pve.operating_system_name,
  pve.operating_system_name_version,
  pve.operating_system_version

from page_view_events pve
