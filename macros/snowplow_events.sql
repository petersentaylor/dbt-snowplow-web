{% macro snowplow_events(start_date, tstamp_field, event_names, app_ids=[], cols=[], additional_filter=none, iab_context=false, ua_context=false, yauaa_context=false) %}
  {{ adapter.dispatch('snowplow_events', 'snowplow_web')(start_date, tstamp_field, event_names, app_ids, cols, additional_filter, iab_context, ua_context, yauaa_context) }}
{% endmacro %}

{% macro default__snowplow_events(start_date, tstamp_field, event_names, app_ids, cols, additional_filter, iab_context, ua_context, yauaa_context) %}

  with latest_session_ids as (

    select distinct
      e.domain_sessionid

    from {{ var('snowplow__events') }} e

    where
      e.domain_sessionid is not null
      and e.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'dvce_created_tstamp') }} -- don't process data that's too late
      and e.collector_tstamp >= 
      {% if is_incremental() %}
        {{ snowplow_utils.timestamp_add('hour',var("snowplow__lookback_window_hours"), snowplow_web.get_start_ts(this, field=tstamp_field)) }}
      {% else %}
        '{{ start_date }}'
      {% endif %}
      {% if event_names|length %}
        and e.event_name in ('{{ event_names|join("','") }}')
      {% endif %}
      {% if app_ids|length %}
        and e.app_id in ('{{ app_ids|join("','") }}')
      {% endif %}
      {% if additional_filter is not none %}
        {{ additional_filter }}
      {% endif %}

  )

  , session_lifecycles as (

    select
      ls.domain_sessionid,
      slm.domain_userid,
      slm.min_collector_tstamp,
      slm.max_collector_tstamp

    from latest_session_ids ls
    inner join {{ ref('snowplow_web_base_sessions_lifecycle_manifest') }} slm
    on ls.domain_sessionid = slm.session_id

  )

  , date_limits as (

    select 
      min(min_collector_tstamp) as lower_limit,
      max(max_collector_tstamp) as upper_limit

    from session_lifecycles

  )

  , prep as (

    select
      e.*,
      dense_rank() over (partition by a.event_id order by a.collector_tstamp) as event_id_dedupe_index --dense_rank so rows with equal tstamps assigned same number

    from {{ var('snowplow__events') }} as e
    inner join session_lifecycles as ns
    on e.domain_sessionid = ns.domain_sessionid
    and e.collector_tstamp between ns.min_collector_tstamp and ns.max_collector_tstamp

    where
      e.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'dvce_created_tstamp') }} -- don't process data that's too late
      and e.collector_tstamp between (select lower_limit from date_limits) and (select upper_limit from date_limits)
      {% if event_names|length %}
        and e.event_name in ('{{ event_names|join("','") }}')
      {% endif %}
      {% if app_ids|length %}
        and e.app_id in ('{{ app_ids|join("','") }}')
      {% endif %}
      {% if additional_filter is not none %}
        {{ additional_filter }}
      {% endif %}

  )

  , events_dedupe as (

    select
      *,
      count(*) over(partition by p.event_id) as row_count

    from prep p
    
    where 
      p.event_id_dedupe_index = 1 -- Keep row(s) with earliest collector_tstamp per dupe event

  )

  , cleaned_events as (

    select *
    from events_dedupe
    where row_count = 1 -- Only keep dupes with single row per earliest collector_tstamp

  )

  , page_context as (

    select
      root_id,
      root_tstamp,
      id as page_view_id

    from {{ var('snowplow__page_view_context') }}
    where 
      root_tstamp between (select lower_limit from date_limits) and (select upper_limit from date_limits)

  )

  {% if var('snowplow__enable_iab') %}

    , iab_context as (

      select
        root_id,
        root_tstamp,
        category,
        primary_impact,
        reason,
        spider_or_robot

      from {{ var('snowplow__iab_context') }}
      where 
        root_tstamp between (select lower_limit from date_limits) and (select upper_limit from date_limits)

    )

  {% endif %}

  {% if var('snowplow__enable_ua') %}

    , ua_parser_context as (

      select
        root_id,
        root_tstamp,
        useragent_family,
        useragent_major,
        useragent_minor,
        useragent_patch,
        useragent_version,
        os_family as os_family_name, --changed name
        os_major,
        os_minor,
        os_patch,
        os_patch_minor,
        os_version,
        device_family

      from {{ var('snowplow__ua_parser_context') }}
      where 
        root_tstamp between (select lower_limit from date_limits) and (select upper_limit from date_limits)

    )

  {% endif %}

  {% if var('snowplow__enable_yauaa') %}

    , yauaa_context as (

      select
        root_id,
        root_tstamp,
        device_class,
        agent_class,
        agent_name,
        agent_name_version,
        agent_name_version_major,
        agent_version,
        agent_version_major,
        device_brand,
        device_name,
        device_version,
        layout_engine_class,
        layout_engine_name,
        layout_engine_name_version,
        layout_engine_name_version_major,
        layout_engine_version,
        layout_engine_version_major,
        operating_system_class,
        operating_system_name,
        operating_system_name_version,
        operating_system_version

      from {{ var('snowplow__yauaa_context') }}
      where 
        root_tstamp between (select lower_limit from date_limits) and (select upper_limit from date_limits)

    )

  {% endif %}

  , events as (

    select
      pc.page_view_id,
    {% if var('snowplow__enable_iab', false) %}
      iab.category,
      iab.primary_impact,
      iab.reason,
      iab.spider_or_robot,
    {% endif %}
    {% if var('snowplow__enable_ua', false) %}
      ua.useragent_family,
      ua.useragent_major,
      ua.useragent_minor,
      ua.useragent_patch,
      ua.useragent_version,
      ua.os_family_name,
      ua.os_major,
      ua.os_minor,
      ua.os_patch,
      ua.os_patch_minor,
      ua.os_version,
      ua.device_family,
    {% endif %}
    {% if var('snowplow__enable_yauaa', false) %}
      yauaa.device_class,
      yauaa.agent_class,
      yauaa.agent_name,
      yauaa.agent_name_version,
      yauaa.agent_name_version_major,
      yauaa.agent_version,
      yauaa.agent_version_major,
      yauaa.device_brand,
      yauaa.device_name,
      yauaa.device_version,
      yauaa.layout_engine_class,
      yauaa.layout_engine_name,
      yauaa.layout_engine_name_version,
      yauaa.layout_engine_name_version_major,
      yauaa.layout_engine_version,
      yauaa.layout_engine_version_major,
      yauaa.operating_system_class,
      yauaa.operating_system_name,
      yauaa.operating_system_name_version,
      yauaa.operating_system_version,
    {% endif %}
    {% if cols|length %}
      {{ cols|join(",\n") }}
    {% else %}
      ce.*
    {% endif %}

    from cleaned_events as ce
    left join page_context as pc
    on ce.event_id = pc.root_id
    and ce.collector_tstamp = pc.root_tstamp
    {% if var('snowplow__enable_iab', false) %}
      left join iab_context as iab
      on ce.event_id = iab.root_id
      and ce.collector_tstamp = iab.root_tstamp
    {% endif %}
    {% if var('snowplow__enable_ua', false) %}
      left join ua_parser_context as ua
      on ce.event_id = ua.root_id
      and ce.collector_tstamp = ua.root_tstamp
    {% endif %}
    {% if var('snowplow__enable_yauaa', false) %}
      left join yauaa_context as yauaa
      on ce.event_id = yauaa.root_id
      and ce.collector_tstamp = yauaa.root_tstamp
    {% endif %}

  )

{% endmacro %}

{% macro snowflake__snowplow_events(start_date, tstamp_field, event_names, app_ids, cols, additional_filter, iab_context, ua_context, yauaa_context) %}

  with latest_session_ids as (

    select distinct
      e.domain_sessionid

    from {{ var('snowplow__events') }} e

    where
      e.domain_sessionid is not null
      and e.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'dvce_created_tstamp') }} -- don't process data that's too late
      and e.collector_tstamp >= 
      {% if is_incremental() %}
        {{ snowplow_utils.timestamp_add('hour',var("snowplow__lookback_window_hours") , snowplow_web.get_start_ts(this, field=tstamp_field)) }}
      {% else %}
        '{{ start_date }}'
      {% endif %}
      {% if event_names|length %}
        and e.event_name in ('{{ event_names|join("','") }}')
      {% endif %}
      {% if app_ids|length %}
        and e.app_id in ('{{ app_ids|join("','") }}')
      {% endif %}
      {% if additional_filter is not none %}
        {{ additional_filter }}
      {% endif %}

  )

  , session_lifecycles as (

    select
      ls.domain_sessionid,
      slm.domain_userid,
      slm.min_collector_tstamp,
      slm.max_collector_tstamp

    from latest_session_ids ls
    inner join {{ ref('snowplow_web_base_sessions_lifecycle_manifest') }} slm
    on ls.domain_sessionid = slm.session_id

  )

  , date_limits as (

    select 
      min(min_collector_tstamp) as lower_limit,
      max(max_collector_tstamp) as upper_limit

    from session_lifecycles

  )

  , events as (

    select
      e.contexts_com_snowplowanalytics_snowplow_web_page_1[0]:id::varchar as page_view_id,
    {% if var('snowplow__enable_iab', false) %}
      e.contexts_com_iab_snowplow_spiders_and_robots_1[0]:category::varchar as category,
      e.contexts_com_iab_snowplow_spiders_and_robots_1[0]:primaryimpact::varchar as primary_impact,
      e.contexts_com_iab_snowplow_spiders_and_robots_1[0]:reason::varchar as reason,
      e.contexts_com_iab_snowplow_spiders_and_robots_1[0]:spiderorrobot::boolean as spider_or_robot,
    {% endif %}
    {% if var('snowplow__enable_ua', false) %}
      e.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:useragentfamily::varchar as useragent_family,
      e.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:useragentmajor::varchar as useragent_major,
      e.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:useragentminor::varchar as useragent_minor,
      e.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:useragentpatch::varchar as useragent_patch,
      e.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:useragentversion::varchar as useragent_version,
      e.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:osfamily::varchar as os_family_name, --NOTE: Changed name to avoid clash with 'os_family' in events table
      e.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:osmajor::varchar as os_major,
      e.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:osminor::varchar as os_minor,
      e.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:ospatch::varchar as os_patch,
      e.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:ospatchminor::varchar as os_patch_minor,
      e.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:osversion::varchar as os_version,
      e.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1[0]:devicefamily::varchar as device_family,
    {% endif %}
    {% if var('snowplow__enable_yauaa', false) %}
      e.contexts_nl_basjes_yauaa_context_1[0]:deviceclass::varchar as device_class,
      e.contexts_nl_basjes_yauaa_context_1[0]:agentclass::varchar as agent_class,
      e.contexts_nl_basjes_yauaa_context_1[0]:agentname::varchar as agent_name,
      e.contexts_nl_basjes_yauaa_context_1[0]:agentnameversion::varchar as agent_name_version,
      e.contexts_nl_basjes_yauaa_context_1[0]:agentnameversionmajor::varchar as agent_name_version_major,
      e.contexts_nl_basjes_yauaa_context_1[0]:agentversion::varchar as agent_version,
      e.contexts_nl_basjes_yauaa_context_1[0]:agentversionmajor::varchar as agent_version_major,
      e.contexts_nl_basjes_yauaa_context_1[0]:devicebrand::varchar as device_brand,
      e.contexts_nl_basjes_yauaa_context_1[0]:devicename::varchar as device_name,
      e.contexts_nl_basjes_yauaa_context_1[0]:deviceversion::varchar as device_version,
      e.contexts_nl_basjes_yauaa_context_1[0]:layoutengineclass::varchar as layout_engine_class,
      e.contexts_nl_basjes_yauaa_context_1[0]:layoutenginename::varchar as layout_engine_name,
      e.contexts_nl_basjes_yauaa_context_1[0]:layoutenginenameversion::varchar as layout_engine_name_version,
      e.contexts_nl_basjes_yauaa_context_1[0]:layoutenginenameversionmajor::varchar as layout_engine_name_version_major,
      e.contexts_nl_basjes_yauaa_context_1[0]:layoutengineversion::varchar as layout_engine_version,
      e.contexts_nl_basjes_yauaa_context_1[0]:layoutengineversionmajor::varchar as layout_engine_version_major,
      e.contexts_nl_basjes_yauaa_context_1[0]:operatingsystemclass::varchar as operating_system_class,
      e.contexts_nl_basjes_yauaa_context_1[0]:operatingsystemname::varchar as operating_system_name,
      e.contexts_nl_basjes_yauaa_context_1[0]:operatingsystemnameversion::varchar as operating_system_name_version,
      e.contexts_nl_basjes_yauaa_context_1[0]:operatingsystemversion::varchar as operating_system_version,
    {% endif %}
    {% if cols|length %}
      {{ cols|join(",\n") }}
    {% else %}
      e.*
    {% endif %}

    from {{ var('snowplow__events') }} as e
    inner join session_lifecycles as ns
    on e.domain_sessionid = ns.domain_sessionid
    and e.collector_tstamp between ns.min_collector_tstamp and ns.max_collector_tstamp

    where
      e.dvce_sent_tstamp <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'dvce_created_tstamp') }} -- don't process data that's too late
      and e.collector_tstamp between (select lower_limit from date_limits) and (select upper_limit from date_limits)
      {% if event_names|length %}
        and e.event_name in ('{{ event_names|join("','") }}')
      {% endif %}
      {% if app_ids|length %}
        and e.app_id in ('{{ app_ids|join("','") }}')
      {% endif %}
      {% if additional_filter is not none %}
        {{ additional_filter }}
      {% endif %}

    qualify row_number() over (partition by e.event_id order by e.collector_tstamp) = 1

  )

{% endmacro %}

