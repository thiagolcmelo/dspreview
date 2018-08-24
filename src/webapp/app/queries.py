# -*- coding: utf-8 -*-
GENERATE_CLASSIFIED = """
INSERT INTO
    dcm_classified (date, brand, sub_brand, dsp, campaign_id, campaign,
                    placement, placement_id, impressions, clicks, reach)
    SELECT
        date,
        brand,
        sub_brand,
        dsp,
        campaign_id,
        campaign,
        placement,
        placement_id,
        impressions,
        clicks,
        reach
    FROM
        (
            SELECT
                raw.date,
                ifnull(classified.brand, "unidentified brand") AS brand,
                ifnull(classified.sub_brand, "unidentified sub brand")
                    AS sub_brand,
                ifnull(classified.dsp, "unidentified dsp") AS dsp,
                raw.campaign_id,
                raw.campaign,
                raw.placement_id,
                raw.placement,
                raw.impressions,
                raw.clicks,
                raw.reach
            FROM
                dcm_raw AS raw
                LEFT JOIN
                    (
                        SELECT
                            id,
                            date,
                            campaign_id,
                            campaign,
                            placement_id,
                            placement,
                            MAX(impressions) AS impressions,
                            MAX(clicks) AS clicks,
                            MAX(reach) AS reach,
                            MAX(brand) brand,
                            MAX(sub_brand) AS sub_brand,
                            MAX(dsp) AS dsp
                        FROM
                            (
                                SELECT
                                    dcm.id AS id,
                                    dcm.date AS date,
                                    dcm.campaign_id AS campaign_id,
                                    dcm.campaign AS campaign,
                                    dcm.placement_id AS placement_id,
                                    dcm.placement AS placement,
                                    dcm.impressions AS impressions,
                                    dcm.clicks AS clicks,
                                    dcm.reach AS reach,
                                    concat(
                                        IF(
                                            cls.use_campaign,
                                            dcm.campaign, ''),
                                        IF(
                                            cls.use_campaign_id,
                                            dcm.campaign_id, ''),
                                        IF(
                                            cls.use_placement_id,
                                            dcm.placement_id, ''),
                                        IF(
                                            cls.use_placement,
                                            dcm.placement, '')
                                        ) AS composed,
                                    cls.pattern AS pattern,
                                    cls.brand AS brand,
                                    cls.sub_brand AS sub_brand,
                                    cls.dsp AS dsp
                                FROM
                                    dcm_raw AS dcm
                                    CROSS JOIN
                                        classifications AS cls
                                HAVING
                                    composed REGEXP pattern
                            )
                            AS big_cross
                        GROUP BY
                            id,
                            date,
                            campaign_id,
                            campaign,
                            placement_id,
                            placement
                    )
                    AS classified
                    ON raw.id = classified.id
        )
        AS big_join;
INSERT INTO
    dsp_classified (date, brand, sub_brand, dsp, campaign_id, campaign,
                    impressions, clicks, cost)
    SELECT
        date,
        brand,
        sub_brand,
        dsp,
        campaign_id,
        campaign,
        impressions,
        clicks,
        cost
    FROM
        (
            SELECT
                raw.date,
                ifnull(classified.brand, "unidentified brand") AS brand,
                ifnull(classified.sub_brand, "unidentified sub brand")
                    AS sub_brand,
                ifnull(classified.dsp, "unidentified dsp") AS dsp,
                raw.campaign_id,
                raw.campaign,
                raw.impressions,
                raw.clicks,
                raw.cost
            FROM
                dsp_raw AS raw
                LEFT JOIN
                    (
                        SELECT
                            id,
                            date,
                            campaign_id,
                            campaign,
                            MAX(impressions) AS impressions,
                            MAX(clicks) AS clicks,
                            MAX(cost) AS cost,
                            MAX(brand) brand,
                            MAX(sub_brand) AS sub_brand,
                            MAX(dsp) AS dsp
                        FROM
                            (
                                SELECT
                                    dsp.id AS id,
                                    dsp.date AS date,
                                    dsp.campaign_id AS campaign_id,
                                    dsp.campaign AS campaign,
                                    dsp.impressions AS impressions,
                                    dsp.clicks AS clicks,
                                    dsp.cost AS cost,
                                    concat(
                                        IF(
                                            cls.use_campaign,
                                            dsp.campaign, ''),
                                        IF(
                                            cls.use_campaign_id,
                                            dsp.campaign_id, '')
                                        ) AS composed,
                                    cls.pattern AS pattern,
                                    cls.brand AS brand,
                                    cls.sub_brand AS sub_brand,
                                    cls.dsp AS dsp
                                FROM
                                    dsp_raw AS dsp
                                    CROSS JOIN
                                        classifications AS cls
                                HAVING
                                    composed REGEXP pattern
                            )
                            AS big_cross
                        GROUP BY
                            id,
                            date,
                            campaign_id,
                            campaign
                    )
                    AS classified
                    ON raw.id = classified.id
        )
        AS big_join;
"""

GENERATE_REPORT = """
INSERT INTO
    report (date, brand, sub_brand, ad_campaign_id, ad_campaign,
            dsp_campaign_id, dsp, dsp_campaign, ad_impressions, ad_clicks,
            ad_reach, dsp_impressions, dsp_clicks, dsp_cost)
    SELECT
        date,
        brand,
        sub_brand,
        ad_campaign_id,
        ad_campaign,
        dsp_campaign_id,
        dsp,
        dsp_campaign,
        ad_impressions,
        ad_clicks,
        ad_reach,
        dsp_impressions,
        dsp_clicks,
        dsp_cost
    FROM
        (
            SELECT
                dcm.date AS date,
                dcm.brand AS brand,
                dcm.sub_brand AS sub_brand,
                dcm.campaign_id AS ad_campaign_id,
                dcm.campaign AS ad_campaign,
                dcm.dsp AS dsp,
                dsp.campaign_id AS dsp_campaign_id,
                dsp.campaign AS dsp_campaign,
                SUM(dcm.impressions) AS ad_impressions,
                SUM(dcm.clicks) AS ad_clicks,
                SUM(dcm.reach) AS ad_reach,
                SUM(dsp.impressions) AS dsp_impressions,
                SUM(dsp.clicks) AS dsp_clicks,
                SUM(dsp.cost) AS dsp_cost
            FROM
                dcm_classified AS dcm
                LEFT JOIN
                    dsp_classified AS dsp
                    ON dcm.date = dsp.date
                    AND dcm.brand = dsp.brand
                    AND dcm.sub_brand = dsp.sub_brand
                    AND dcm.dsp = dsp.dsp
            GROUP BY
                dcm.date,
                dcm.brand,
                dcm.sub_brand,
                dcm.campaign_id,
                dcm.campaign,
                dcm.dsp,
                dsp.campaign_id,
                dsp.campaign
        )
        AS big_join
        ON DUPLICATE KEY
        UPdate
            report.ad_impressions = big_join.ad_impressions,
            report.ad_clicks = big_join.ad_clicks,
            report.ad_reach = big_join.ad_reach,
            report.dsp_impressions = big_join.dsp_impressions,
            report.dsp_clicks = big_join.dsp_clicks,
            report.dsp_cost = big_join.dsp_cost,
            report.updated_at = CURRENT_TIMESTAMP();
"""
