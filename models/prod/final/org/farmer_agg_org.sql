{{ config(
  materialized='table',
  tags=["final","final_org"]
) }}

SELECT DISTINCT
    COALESCE(CAST(n.date_time AS DATE), CAST(g.date_time AS DATE), CAST(gd.date_time AS DATE)) AS date_time,
    COALESCE(CAST(n.dam AS TEXT), CAST(g.dam AS TEXT), CAST(gd.dam AS TEXT)) AS dam,
    COALESCE(CAST(n.state AS TEXT), CAST(g.state AS TEXT), CAST(gd.state AS TEXT)) AS state,
    COALESCE(CAST(n.district AS TEXT), CAST(g.district AS TEXT), CAST(gd.district AS TEXT)) AS district,
    COALESCE(CAST(n.taluka AS TEXT), CAST(g.taluka AS TEXT), CAST(gd.taluka AS TEXT)) AS taluka,
    COALESCE(CAST(n.village AS TEXT), CAST(g.village AS TEXT), CAST(gd.village AS TEXT)) AS village,
    COALESCE(CAST(n.ngo_name AS TEXT), CAST(g.ngo_name AS TEXT), CAST(gd.ngo_name AS TEXT)) AS ngo_name,
    COALESCE(n.verified_farmers, g.verified_farmers, gd.verified_farmers, 0) AS verified_farmers,
    COALESCE(n.unverified_farmers, g.unverified_farmers, gd.unverified_farmers, 0) AS unverified_farmers,
    COALESCE(n.total, g.total, gd.total, 0) AS total,
    COALESCE(n.vulnerable_marginal, g.vulnerable_marginal, gd.vulnerable_marginal, 0) AS vulnerable_marginal,
    COALESCE(n.vulnerable_small, g.vulnerable_small, gd.vulnerable_small, 0) AS vulnerable_small,
    COALESCE(n.semi_medium, g.semi_medium, gd.semi_medium, 0) AS semi_medium,
    COALESCE(n.medium, g.medium, gd.medium, 0) AS medium,
    COALESCE(n.large, g.large, gd.large, 0) AS large,
    COALESCE(CAST(n.widow AS NUMERIC), CAST(gd.widow AS NUMERIC), 0) AS widow,
    COALESCE(CAST(n.disabled AS NUMERIC), CAST(gd.disabled AS NUMERIC), 0) AS disabled,
    COALESCE(CAST(n.family_of_farmer_who_committed_suicide AS NUMERIC), CAST(gd.family_of_farmer_who_committed_suicide AS NUMERIC), 0) AS family_of_farmer_who_committed_suicide,
    COALESCE(n.farmer_niti_22, 0) AS farmer_niti_22,
    CASE 
        WHEN n.date_time IS NOT NULL THEN 'Niti Aayog'
        WHEN g.date_time IS NOT NULL THEN 'GDGS'
        ELSE 'Project A'
    END AS project
FROM 
    {{ ref('farmer_niti_agg_union') }} n
FULL OUTER JOIN 
    {{ ref('farmer_agg_gramin') }} g
ON 
    n.date_time = g.date_time AND
    n.dam = g.dam AND
    n.state = g.state AND
    n.district = g.district AND
    n.taluka = g.taluka AND
    n.village = g.village AND
    n.ngo_name = g.ngo_name
FULL OUTER JOIN 
    {{ ref('farmer_gdgs_agg_union') }} gd
ON 
    COALESCE(n.date_time, g.date_time) = gd.date_time AND
    COALESCE(n.dam, g.dam) = gd.dam AND
    COALESCE(n.state, g.state) = gd.state AND
    COALESCE(n.district, g.district) = gd.district AND
    COALESCE(n.taluka, g.taluka) = gd.taluka AND
    COALESCE(n.village, g.village) = gd.village AND
    COALESCE(n.ngo_name, g.ngo_name) = gd.ngo_name