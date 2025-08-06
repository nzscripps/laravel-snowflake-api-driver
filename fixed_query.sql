-- Fixed query
SELECT DISTINCT
    ARRAY_AGG(DATE_CATEGORY) within group (order by DATE_CATEGORY asc) AS DATE_CATEGORY,
    ARRAY_AGG(OCTANE_IMP) within group (order by DATE_CATEGORY asc) AS OCTANE_IMP,
    ARRAY_AGG(TV_IMP) within group (order by DATE_CATEGORY asc) AS TV_IMP,
    ARRAY_AGG(TV_IMP_HH) within group (order by DATE_CATEGORY asc) AS TV_IMP_HH,
    ARRAY_AGG(USERS) within group (order by DATE_CATEGORY asc) AS USERS,
    ARRAY_AGG(NEW_USERS_FROM_GA4) within group (order by DATE_CATEGORY asc) AS NEW_USERS,
    ARRAY_AGG(SESSIONS_FROM_GA4) within group (order by DATE_CATEGORY asc) AS SESSIONS,
    ARRAY_AGG(CLICKS_TO_DRIVING_DIRECTIONS) within group (order by DATE_CATEGORY asc) AS CLICKS_TO_DRIVING_DIRECTIONS,
    ARRAY_AGG(CLICKS_TO_PHONE_NUMBER) within group (order by DATE_CATEGORY asc) AS CLICKS_TO_PHONE_NUMBER,
    ARRAY_AGG(CLICKS_TO_WEBSITE) within group (order by DATE_CATEGORY asc) AS CLICKS_TO_WEBSITE,
    ARRAY_AGG(DIRECT_SEARCHES) within group (order by DATE_CATEGORY asc) AS DIRECT_SEARCHES,
    ARRAY_AGG(UNIVERSAL_PIXEL) within group (order by DATE_CATEGORY asc) AS UNIVERSAL_PIXEL,
    ARRAY_AGG(GOAL_COMPLETIONS) within group (order by DATE_CATEGORY asc) AS GOAL_COMPLETIONS,
    ARRAY_AGG(PAGEVIEWS) within group (order by DATE_CATEGORY asc) AS PAGEVIEWS,
    ARRAY_AGG(AUDIO_DELIV_IMP) within group (order by DATE_CATEGORY asc) AS AUDIO_DELIV_IMP,
    ARRAY_AGG(HYPERLOCAL_DELIV_IMP) within group (order by DATE_CATEGORY asc) AS HYPERLOCAL_DELIV_IMP,
    ARRAY_AGG(WEATHER_CHANNEL_DELIV_IMP) within group (order by DATE_CATEGORY asc) AS WEATHER_CHANNEL_DELIV_IMP,
    ARRAY_AGG(GOOGLE_SEARCH_DELIV_IMP) within group (order by DATE_CATEGORY asc) AS GOOGLE_SEARCH_DELIV_IMP,
    ARRAY_AGG(SCRIPPS_TARGETED_EMAILS_DELIV_IMP) within group (order by DATE_CATEGORY asc) AS SCRIPPS_TARGETED_EMAILS_DELIV_IMP,
    ARRAY_AGG(SCRIPPS_O_AND_O_DELIV_IMP) within group (order by DATE_CATEGORY asc) AS SCRIPPS_O_AND_O_DELIV_IMP,
    ARRAY_AGG(YOUTUBE_AD_NETWORK_DELIV_IMP) within group (order by DATE_CATEGORY asc) AS YOUTUBE_AD_NETWORK_DELIV_IMP,
    ARRAY_AGG(STN_DELIV_IMP) within group (order by DATE_CATEGORY asc) AS STN_DELIV_IMP,
    ARRAY_AGG(FACEBOOK_DELIV_IMP) within group (order by DATE_CATEGORY asc) AS FACEBOOK_DELIV_IMP,
    ARRAY_AGG(GOOGLE_DISPLAY_AD_NETWORK_DELIV_IMP) within group (order by DATE_CATEGORY asc) AS GOOGLE_DISPLAY_AD_NETWORK_DELIV_IMP,
    ARRAY_AGG(OTHER_PRODUCT_DELIV_IMP) within group (order by DATE_CATEGORY asc) AS OTHER_PRODUCT_DELIV_IMP,
    ARRAY_AGG("All Pixels") within group (order by DATE_CATEGORY asc) AS "All Pixels", 
    ARRAY_AGG("'9e90t3nOfBJS3oQuFn7MzI3v0G1s'") within group (order by DATE_CATEGORY asc) AS "9e90t3nOfBJS3oQuFn7MzI3v0G1s", 
    ARRAY_AGG("'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'") within group (order by DATE_CATEGORY asc) AS "Wzlyy2fuuDSUiTmzhqIq4dGVV1QB"
FROM
    (SELECT DISTINCT
        DATE_TRUNC('WEEK',TO_DATE(CAL_DATE)) AS DATE_CATEGORY,
        SUM(CASE WHEN OCTANE_IMP IS NULL THEN 0 ELSE OCTANE_IMP END) AS OCTANE_IMP,
        SUM(CASE WHEN pixels.TV_IMP IS NULL THEN 0 ELSE ROUND(pixels.TV_IMP, 0) END) AS TV_IMP,
        SUM(CASE WHEN pixels.TV_IMP_HH IS NULL THEN 0 ELSE ROUND(pixels.TV_IMP_HH, 0) END) AS TV_IMP_HH,
        SUM(CASE WHEN USERS IS NULL THEN 0 ELSE USERS END) AS USERS,
        SUM(CASE WHEN NEW_USERS IS NULL THEN 0 ELSE NEW_USERS END) AS NEW_USERS,
        SUM(CASE WHEN SESSIONS IS NULL THEN 0 ELSE SESSIONS END) AS SESSIONS,
        SUM(CASE WHEN CLICKS_TO_DRIVING_DIRECTIONS IS NULL THEN 0 ELSE CLICKS_TO_DRIVING_DIRECTIONS END) AS CLICKS_TO_DRIVING_DIRECTIONS,
        SUM(CASE WHEN CLICKS_TO_PHONE_NUMBER IS NULL THEN 0 ELSE CLICKS_TO_PHONE_NUMBER END) AS CLICKS_TO_PHONE_NUMBER,
        SUM(CASE WHEN CLICKS_TO_WEBSITE IS NULL THEN 0 ELSE CLICKS_TO_WEBSITE END) AS CLICKS_TO_WEBSITE,
        SUM(CASE WHEN DIRECT_SEARCHES IS NULL THEN 0 ELSE DIRECT_SEARCHES END) AS DIRECT_SEARCHES,
        SUM(CASE WHEN VISITS_ATTRIBUTED IS NULL THEN 0 ELSE VISITS_ATTRIBUTED END) AS UNIVERSAL_PIXEL,
        SUM(CASE WHEN GOAL_COMPLETIONS IS NULL THEN 0 ELSE GOAL_COMPLETIONS END) AS GOAL_COMPLETIONS,
        SUM(CASE WHEN PAGEVIEWS IS NULL THEN 0 ELSE PAGEVIEWS END) AS PAGEVIEWS,
        SUM(CASE WHEN AUDIO_DELIV_IMP IS NULL THEN 0 ELSE AUDIO_DELIV_IMP END) AS AUDIO_DELIV_IMP,
        SUM(CASE WHEN HYPERLOCAL_DELIV_IMP IS NULL THEN 0 ELSE HYPERLOCAL_DELIV_IMP END) AS HYPERLOCAL_DELIV_IMP,
        SUM(CASE WHEN WEATHER_CHANNEL_DELIV_IMP IS NULL THEN 0 ELSE WEATHER_CHANNEL_DELIV_IMP END) AS WEATHER_CHANNEL_DELIV_IMP,
        SUM(CASE WHEN GOOGLE_SEARCH_DELIV_IMP IS NULL THEN 0 ELSE GOOGLE_SEARCH_DELIV_IMP END) AS GOOGLE_SEARCH_DELIV_IMP,
        SUM(CASE WHEN SCRIPPS_TARGETED_EMAILS_DELIV_IMP IS NULL THEN 0 ELSE SCRIPPS_TARGETED_EMAILS_DELIV_IMP END) AS SCRIPPS_TARGETED_EMAILS_DELIV_IMP,
        SUM(CASE WHEN SCRIPPS_O_AND_O_DELIV_IMP IS NULL THEN 0 ELSE SCRIPPS_O_AND_O_DELIV_IMP END) AS SCRIPPS_O_AND_O_DELIV_IMP,
        SUM(CASE WHEN YOUTUBE_AD_NETWORK_DELIV_IMP IS NULL THEN 0 ELSE YOUTUBE_AD_NETWORK_DELIV_IMP END) AS YOUTUBE_AD_NETWORK_DELIV_IMP,
        SUM(CASE WHEN STN_DELIV_IMP IS NULL THEN 0 ELSE STN_DELIV_IMP END) AS STN_DELIV_IMP,
        SUM(CASE WHEN FACEBOOK_DELIV_IMP IS NULL THEN 0 ELSE FACEBOOK_DELIV_IMP END) AS FACEBOOK_DELIV_IMP,
        SUM(CASE WHEN GOOGLE_DISPLAY_AD_NETWORK_DELIV_IMP IS NULL THEN 0 ELSE GOOGLE_DISPLAY_AD_NETWORK_DELIV_IMP END) AS GOOGLE_DISPLAY_AD_NETWORK_DELIV_IMP,
        SUM(CASE WHEN OTHER_PRODUCT_DELIV_IMP IS NULL THEN 0 ELSE OTHER_PRODUCT_DELIV_IMP END) AS OTHER_PRODUCT_DELIV_IMP, 
        SUM(IFNULL("All Pixels", 0)) as "All Pixels", 
        SUM(IFNULL("'9e90t3nOfBJS3oQuFn7MzI3v0G1s'", 0)) as "'9e90t3nOfBJS3oQuFn7MzI3v0G1s'", 
        SUM(IFNULL("'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'", 0)) as "'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'",
        SUM(NEW_USERS_FROM_GA4) AS NEW_USERS_FROM_GA4,
        SUM(SESSIONS_FROM_GA4) AS SESSIONS_FROM_GA4
    FROM ((WITH RECURSIVE nrows(cal_date) AS (
            SELECT to_date('2025-04-01')
            UNION ALL
            SELECT dateadd(day, 1, cal_date)
            FROM nrows
            WHERE cal_date < '2025-04-22'
            )
            SELECT cal_date FROM nrows)) dates
    LEFT JOIN (SELECT DISTINCT OCTANE_IMP
        , VISITS_ATTRIBUTED
        , STATION_NAME
        , ADVERTISER_NAME
        , UI_VP_VERIFY.DATE
        FROM SFPRDH.VIEWER_PREDICT.UI_VP_VERIFY
        WHERE ADVERTISER_NAME = 'Royal Gorge Route Railroad - Direct'
            and UI_VP_VERIFY.DATE between '2025-04-01' and '2025-04-22') ui_vp_stations
        on ui_vp_stations.ADVERTISER_NAME = 'Royal Gorge Route Railroad - Direct'
            AND ui_vp_stations.STATION_NAME in ('KOAA')
            AND TO_DATE(ui_vp_stations.DATE) = dates.cal_date
    LEFT JOIN (
        SELECT
            UI_VP_VERIFY.ADVERTISER_NAME,
            UI_VP_VERIFY.DATE,
            USERS,
            UI_VP_VERIFY.NEW_USERS,
            SESSIONS,
            CLICKS_TO_DRIVING_DIRECTIONS,
            CLICKS_TO_PHONE_NUMBER,
            CLICKS_TO_WEBSITE,
            DIRECT_SEARCHES,
            GOAL_COMPLETIONS,
            PAGEVIEWS,
            AUDIO_DELIV_IMP,
            HYPERLOCAL_DELIV_IMP,
            WEATHER_CHANNEL_DELIV_IMP,
            GOOGLE_SEARCH_DELIV_IMP,
            SCRIPPS_TARGETED_EMAILS_DELIV_IMP,
            SCRIPPS_O_AND_O_DELIV_IMP,
            OCTANE_DELIV_IMP,
            YOUTUBE_AD_NETWORK_DELIV_IMP,
            STN_DELIV_IMP,
            FACEBOOK_DELIV_IMP,
            GOOGLE_DISPLAY_AD_NETWORK_DELIV_IMP,
            OTHER_PRODUCT_DELIV_IMP
            , CASE WHEN ga4.new_users IS NULL THEN 0 ELSE ga4.new_users END AS NEW_USERS_FROM_GA4
            , CASE WHEN ga4.daily_sessions IS NULL THEN 0 ELSE ga4.daily_sessions END AS SESSIONS_FROM_GA4
        FROM SFPRDH.VIEWER_PREDICT.UI_VP_VERIFY
        LEFT JOIN (
            SELECT
                Client,
                Date,
                SUM(new_users) AS new_users,
                SUM(daily_sessions) AS daily_sessions
            FROM SFPRDH.TAPCLICKS.STG_GA4_SOURCE_MEDIUM
            WHERE 1=1
                
            GROUP BY
                Client,
                Date
        ) AS ga4
        ON ga4.Client = UI_VP_VERIFY.Client
        AND ga4.Date = UI_VP_VERIFY.Date
        WHERE ADVERTISER_NAME = 'Royal Gorge Route Railroad - Direct'
            and UI_VP_VERIFY.DATE between '2025-04-01' and '2025-04-22'
            and UI_VP_VERIFY.STATION_NAME IN ('KOAA')
    ) ui_vp_default
    on ui_vp_default.ADVERTISER_NAME = 'Royal Gorge Route Railroad - Direct'
        AND TO_DATE(ui_vp_default.DATE) = dates.cal_date
    LEFT JOIN (SELECT PIXELS_DATE,
                  SUM(IFNULL(TV_IMP, 0)) as TV_IMP,
                  SUM(IFNULL(TV_IMP_HH, 0)) as TV_IMP_HH,
                  SUM(IFNULL("All Pixels", 0)) as "All Pixels", 
                  SUM(IFNULL("'9e90t3nOfBJS3oQuFn7MzI3v0G1s'", 0)) as "'9e90t3nOfBJS3oQuFn7MzI3v0G1s'", 
                  SUM(IFNULL("'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'", 0)) as "'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'"
                FROM (SELECT DISTINCT
                    TO_DATE(LEFT(spots.START_TIME_LOCAL,10), 'YYYY-MM-DD') AS ACTUAL_AIR_DATE
                  , to_varchar(spots.START_TIME_LOCAL::DATE,'mm-dd-yy') AS ACTUAL_AIR_DATE2
                  , TO_VARCHAR(TO_TIME(spots.START_TIME_LOCAL::TIMESTAMP_TZ),'HH12:MI:SS AM') AS AIR_TIME
                  , spots.CREATIVE_RESOURCE_ID
                  , spots.VIDEO_URL
                  , IFF(spots.EXTERNAL_ASRUN_URL is NULL, NULL, concat(spots.EXTERNAL_ASRUN_URL, '&AU=', EXTERNAL_SESSIONS.SESSION_ID)) as EXTERNAL_VIDEO_URL
                  , spots.THUMBNAIL_URL
                  , ROUND(IFNULL(spots.CS_IMPRESSION_DEMO,0)) as TV_IMP
                  , IFNULL(IFNULL(spots.SPOT_EXTERNAL_AMOUNT, spots.MEDIA_VALUE), 0) AS RATE
                  , IFNULL(spots.CS_IMPRESSION_HH,0) as TV_IMP_HH
                  , spots.START_TIME_LOCAL as AIR_TIME_ACTUAL
                  , IFNULL(spots.CS_SERIES_NAME, spots.WO_PROGRAM_NAME) as Program
                  , spots.CLIENT_DEMO as DEMO
                  , spots.AFFILIATE as Network
                  , spots.COMSCORE_STATION as Station
                  , dayname(spots.START_TIME_LOCAL) as Day
                  , DAYPART as Daypart
                  , IFNULL(spots.INVOICE_ISCI_CODE1, spots.TITLE)  as TITLE
                  , pixelFires.*
                  , ARRAY_AGG(spots.SIGNAL_NONFRAC) as SIGNAL_FRAC
                  , ARRAY_AGG(spots.BASELINE_NONFRAC) as BASELINE_FRAC
                  , creative_info.DURATION_SECONDS as CREATIVE_DURATION
                  FROM SFPRDH.DATA_SCIENCE.KINETIQ_WOT_MADHIVE_PIXEL_ATTRIBUTION_VERIFY_WAVELETS spots
                  LEFT JOIN (SELECT ID,
                             TO_DATE(SUBSTR(START_TIME_LOCAL, 1,10)) as PIXELS_DATE,
                             START_TIME_LOCAL as PIXEL_START_TIME_LOCAL,
                             COMSCORE_STATION,
                             IFNULL("'9e90t3nOfBJS3oQuFn7MzI3v0G1s'", 0) as "'9e90t3nOfBJS3oQuFn7MzI3v0G1s'", 
                             IFNULL("'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'", 0) as "'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'",
                             IFNULL("'9e90t3nOfBJS3oQuFn7MzI3v0G1s'",0) + IFNULL("'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'",0) as "All Pixels"
                      FROM (
                        SELECT CEIL(DIFF_FRAC_FLOOR) as DIFF_FRAC_FLOOR, ID, BEACON_ID, START_TIME_LOCAL, COMSCORE_STATION
                        FROM SFPRDH.DATA_SCIENCE.KINETIQ_WOT_MADHIVE_PIXEL_ATTRIBUTION_VERIFY_WAVELETS
                        WHERE COMSCORE_STATION in ('KOAA') and WOT_NAME = 'Royal Gorge Route Railroad - Direct'
                          and TO_DATE(SUBSTR(START_TIME_LOCAL, 1,10)) between '2025-04-01' and '2025-04-22'
                      )
                      PIVOT(SUM(DIFF_FRAC_FLOOR) FOR BEACON_ID IN ('9e90t3nOfBJS3oQuFn7MzI3v0G1s','Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'))
                  ) pixelFires on pixelFires.PIXEL_START_TIME_LOCAL = spots.START_TIME_LOCAL AND pixelFires.COMSCORE_STATION = spots.COMSCORE_STATION
                  LEFT JOIN (SELECT TOP 1 SESSION_ID FROM DATA_SCIENCE.KINETIQ_SESSION_ID ORDER BY TIMESTAMP_UTC DESC) EXTERNAL_SESSIONS on 1 = 1
                  LEFT JOIN (
                      SELECT DISTINCT t1.CREATIVE_RESOURCE_ID, MAX(t2.DURATION_SECONDS) AS DURATION_SECONDS
                      FROM SFPRDH.DATA_SCIENCE.KINETIQ_WOT_MADHIVE_PIXEL_ATTRIBUTION_VERIFY_WAVELETS t1
                      LEFT JOIN (
                        SELECT DISTINCT CREATIVE_RESOURCE_ID AS KINETIQ_CREATIVE_RESOURCE_ID, DURATION_SECONDS
                        FROM SFDVDH.ZDATA_SCIENCE.KINETIQ_OCCURRENCES
                        WHERE DURATION_SECONDS > 0
                      ) AS t2 ON t1.CREATIVE_RESOURCE_ID = t2.KINETIQ_CREATIVE_RESOURCE_ID
                      WHERE t1.CREATIVE_RESOURCE_ID IS NOT NULL
                      GROUP BY t1.CREATIVE_RESOURCE_ID
                  ) AS creative_info ON spots.CREATIVE_RESOURCE_ID = creative_info.CREATIVE_RESOURCE_ID
                  WHERE spots.WOT_NAME = 'Royal Gorge Route Railroad - Direct'
                      and spots.COMSCORE_STATION in ('KOAA')
                      and TO_DATE(SUBSTR(START_TIME_LOCAL, 1,10)) between '2025-04-01' and '2025-04-22'
                  GROUP BY to_varchar(spots.START_TIME_LOCAL::DATE,'mm-dd-yy')
                  , TO_VARCHAR(TO_TIME(spots.START_TIME_LOCAL::TIMESTAMP_TZ),'HH12:MI:SS AM')
                  , spots.CREATIVE_RESOURCE_ID
                  , spots.VIDEO_URL
                  , IFF(spots.EXTERNAL_ASRUN_URL is NULL, NULL, concat(spots.EXTERNAL_ASRUN_URL, '&AU=', EXTERNAL_SESSIONS.SESSION_ID))
                  , spots.THUMBNAIL_URL
                  , IFNULL(spots.CS_IMPRESSION_DEMO,0)
                  , IFNULL(spots.CS_IMPRESSION_HH,0)
                  , IFNULL(IFNULL(spots.SPOT_EXTERNAL_AMOUNT, spots.MEDIA_VALUE), 0)
                  , spots.START_TIME_LOCAL
                  , IFNULL(spots.CS_SERIES_NAME, spots.WO_PROGRAM_NAME)
                  , spots.CLIENT_DEMO
                  , spots.AFFILIATE
                  , spots.COMSCORE_STATION
                  , dayname(spots.START_TIME_LOCAL)
                  , DAYPART
                  , IFNULL(spots.INVOICE_ISCI_CODE1, spots.TITLE)
                  , pixelFires.ID
                  , pixelFires.PIXELS_DATE
                  , pixelFires.PIXEL_START_TIME_LOCAL
                  , pixelFires.COMSCORE_STATION
                  , pixelFires."All Pixels", pixelFires."'9e90t3nOfBJS3oQuFn7MzI3v0G1s'", pixelFires."'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'"
                  , creative_info.DURATION_SECONDS
                )
                GROUP BY PIXELS_DATE
    ) pixels on PIXELS_DATE = dates.cal_date
    WHERE 1=1
       AND TO_DATE(CAL_DATE) <= '2025-04-22'
       AND TO_DATE(CAL_DATE) >= '2025-04-01'
    GROUP BY DATE_CATEGORY
    ORDER BY DATE_CATEGORY
    ) a
ORDER BY DATE_CATEGORY
; 