PRAGMA foreign_keys=OFF;

DROP VIEW IF EXISTS vw_fd_latest_submission;
CREATE VIEW vw_fd_latest_submission AS
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ACCESSIONNUMBER
               ORDER BY FILING_DATE DESC NULLS LAST, ROWID DESC
           ) AS rn
    FROM stg_fd_FORMDSUBMISSION
)
SELECT ACCESSIONNUMBER,
       SUBMISSIONTYPE,
       FILING_DATE,
       FILE_NUM,
       SIC_CODE,
       TESTORLIVE
FROM ranked
WHERE rn = 1;

DROP VIEW IF EXISTS vw_cf_latest_submission;
CREATE VIEW vw_cf_latest_submission AS
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ACCESSION_NUMBER
               ORDER BY FILING_DATE DESC NULLS LAST, ROWID DESC
           ) AS rn
    FROM stg_cf_FORM_C_SUBMISSION
)
SELECT ACCESSION_NUMBER,
       SUBMISSION_TYPE,
       FILING_DATE,
       CIK,
       FILE_NUMBER,
       PERIOD
FROM ranked
WHERE rn = 1;

DROP VIEW IF EXISTS vw_adv_latest;
CREATE VIEW vw_adv_latest AS
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY FilingID
               ORDER BY DateSubmitted DESC NULLS LAST, ROWID DESC
           ) AS rn
    FROM stg_adv_base_a
)
SELECT *
FROM ranked
WHERE rn = 1;

DROP VIEW IF EXISTS vw_fd_latest_offering;
CREATE VIEW vw_fd_latest_offering AS
SELECT ls.ACCESSIONNUMBER,
       ls.SUBMISSIONTYPE,
       ls.FILING_DATE,
       ls.FILE_NUM,
       fo.INDUSTRYGROUPTYPE,
       fo.FEDERALEXEMPTIONS_ITEMS_LIST,
       fo.ISEQUITYTYPE,
       fo.ISDEBTTYPE,
       fo.ISPOOLEDINVESTMENTFUNDTYPE,
       fo.HASNONACCREDITEDINVESTORS,
       fo.TOTALOFFERINGAMOUNT,
       fo.TOTALAMOUNTSOLD,
       fo.TOTALREMAINING,
       fo.MINIMUMINVESTMENTACCEPTED,
       fo.SALE_DATE,
       isr.CIK,
       isr.ENTITYNAME,
       isr.ENTITYTYPE,
       isr.CITY,
       isr.STATEORCOUNTRY,
       isr.STATEORCOUNTRYDESCRIPTION
FROM vw_fd_latest_submission ls
LEFT JOIN stg_fd_OFFERING fo USING (ACCESSIONNUMBER)
LEFT JOIN stg_fd_ISSUERS isr USING (ACCESSIONNUMBER);

DROP VIEW IF EXISTS vw_cf_latest_offering;
CREATE VIEW vw_cf_latest_offering AS
SELECT ls.ACCESSION_NUMBER,
       ls.SUBMISSION_TYPE,
       ls.FILING_DATE,
       iss.NAMEOFISSUER,
       iss.LEGALSTATUSFORM,
       iss.CITY,
       iss.STATEORCOUNTRY,
       iss.ISSUERWEBSITE,
       iss.PROGRESSUPDATE,
       dis.SECURITYOFFEREDTYPE,
       dis.NOOFSECURITYOFFERED,
       dis.PRICE,
       dis.OFFERINGAMOUNT,
       dis.MAXIMUMOFFERINGAMOUNT,
       dis.OVERSUBSCRIPTIONACCEPTED,
       dis.OVERSUBSCRIPTIONALLOCATIONTYPE,
       dis.DEADLINEDATE,
       dis.CURRENTEMPLOYEES,
       dis.TOTALASSETMOSTRECENTFISCALYEAR,
       dis.REVENUEMOSTRECENTFISCALYEAR,
       dis.NETINCOMEMOSTRECENTFISCALYEAR,
       dis.TOTALASSETPRIORYEAR,
       dis.REVENUEPRIORYEAR,
       dis.NETINCOMEPRIORYEAR
FROM vw_cf_latest_submission ls
LEFT JOIN stg_cf_FORM_C_ISSUER_INFORMATION iss USING (ACCESSION_NUMBER)
LEFT JOIN stg_cf_FORM_C_DISCLOSURE dis USING (ACCESSION_NUMBER);

DROP VIEW IF EXISTS vw_fd_features;
CREATE VIEW vw_fd_features AS
WITH parsed AS (
    SELECT l.*,
           CAST(REPLACE(REPLACE(fo.TOTALOFFERINGAMOUNT, ',', ''), '$', '') AS REAL) AS target_raise,
           CAST(REPLACE(REPLACE(fo.TOTALAMOUNTSOLD, ',', ''), '$', '') AS REAL) AS amount_sold,
           CAST(REPLACE(REPLACE(fo.MINIMUMINVESTMENTACCEPTED, ',', ''), '$', '') AS REAL) AS min_invest
    FROM vw_fd_latest_offering l
    LEFT JOIN stg_fd_OFFERING fo USING (ACCESSIONNUMBER)
)
SELECT ACCESSIONNUMBER,
       SUBMISSIONTYPE,
       FILING_DATE,
       INDUSTRYGROUPTYPE,
       FEDERALEXEMPTIONS_ITEMS_LIST,
       STATEORCOUNTRYDESCRIPTION AS issuer_state,
       target_raise,
       amount_sold,
       (CASE WHEN target_raise > 0 THEN amount_sold / target_raise ELSE NULL END) AS sold_vs_target,
       min_invest,
       CASE
           WHEN min_invest IS NULL THEN 'Unknown'
           WHEN min_invest >= 1000000 THEN '>= $1M'
           WHEN min_invest >= 100000 THEN '$100k-$1M'
           WHEN min_invest >= 10000 THEN '$10k-$100k'
           WHEN min_invest > 0 THEN '< $10k'
           ELSE 'Unknown'
       END AS min_invest_bucket,
       COALESCE(ISEQUITYTYPE, 0) AS is_equity,
       COALESCE(ISDEBTTYPE, 0) AS is_debt,
       COALESCE(ISPOOLEDINVESTMENTFUNDTYPE, 0) AS is_pooled,
       COALESCE(HASNONACCREDITEDINVESTORS, 0) AS allows_non_accredited
FROM parsed;

DROP VIEW IF EXISTS vw_cf_features;
CREATE VIEW vw_cf_features AS
WITH parsed AS (
    SELECT l.*,
           CAST(REPLACE(REPLACE(l.OFFERINGAMOUNT, ',', ''), '$', '') AS REAL) AS target_raise,
           CAST(REPLACE(REPLACE(l.MAXIMUMOFFERINGAMOUNT, ',', ''), '$', '') AS REAL) AS max_raise,
           CAST(REPLACE(REPLACE(l.PRICE, ',', ''), '$', '') AS REAL) AS unit_price,
           CAST(REPLACE(l.CURRENTEMPLOYEES, ',', '') AS REAL) AS employees
    FROM vw_cf_latest_offering l
)
SELECT ACCESSION_NUMBER,
       SUBMISSION_TYPE,
       FILING_DATE,
       NAMEOFISSUER,
       STATEORCOUNTRY,
       SECURITYOFFEREDTYPE,
       target_raise,
       max_raise,
       CASE WHEN max_raise > 0 THEN target_raise / max_raise ELSE NULL END AS target_vs_cap,
       unit_price,
       CASE
           WHEN unit_price IS NULL THEN 'Unknown'
           WHEN unit_price >= 100 THEN '>= $100'
           WHEN unit_price >= 10 THEN '$10-$100'
           WHEN unit_price >= 1 THEN '$1-$10'
           WHEN unit_price > 0 THEN '< $1'
           ELSE 'Unknown'
       END AS price_bucket,
       employees,
       CASE
           WHEN employees IS NULL THEN 'Unknown'
           WHEN employees >= 50 THEN '50+'
           WHEN employees >= 10 THEN '10-49'
           WHEN employees >= 5 THEN '5-9'
           WHEN employees >= 1 THEN '1-4'
           ELSE '0'
       END AS employee_band,
       OVERSUBSCRIPTIONACCEPTED,
       OVERSUBSCRIPTIONALLOCATIONTYPE
FROM parsed;

DROP VIEW IF EXISTS vw_adv_features;
CREATE VIEW vw_adv_features AS
SELECT a.FilingID,
       a."1A" AS adviser_name,
       a."1F1-City" AS city,
       a."1F1-State" AS state,
       a."1F1-Country" AS country,
       a."5F2c" AS total_raum,
       CASE
           WHEN a."5F2c" >= 10000000000 THEN '>$10B'
           WHEN a."5F2c" >= 1000000000 THEN '$1B-$10B'
           WHEN a."5F2c" >= 100000000 THEN '$100M-$1B'
           WHEN a."5F2c" >= 10000000 THEN '$10M-$100M'
           WHEN a."5F2c" IS NOT NULL THEN '<$10M'
           ELSE 'Unknown'
       END AS raum_bucket,
       a."5D1a" AS non_hnw_clients,
       a."5D1b" AS hnw_clients,
       a."5D1e" AS pooled_clients,
       a."5D1f" AS pension_clients,
       a."5K1" AS is_broker_dealer,
       a."5K2" AS is_bank_affiliate,
       a."5K3" AS is_insurance_affiliate,
       a."5K4" AS is_other_affiliate,
       a."7A12" AS commodity_pool_flag,
       a."7A16" AS sponsor_flag,
       b."2-SECStateReg" AS registered_states
FROM vw_adv_latest a
LEFT JOIN stg_adv_base_b b USING (FilingID);

DROP VIEW IF EXISTS vw_investor_deal_candidates;
CREATE VIEW vw_investor_deal_candidates AS
WITH deal_universe AS (
    SELECT 'FORM_D' AS deal_type,
           'FD:' || f.ACCESSIONNUMBER AS deal_id,
           f.ACCESSIONNUMBER AS accession_id,
           o.ENTITYNAME AS issuer_name,
           UPPER(TRIM(COALESCE(o.STATEORCOUNTRY, o.STATEORCOUNTRYDESCRIPTION))) AS issuer_state,
           f.target_raise,
           f.min_invest_bucket AS ticket_hint,
           f.INDUSTRYGROUPTYPE AS industry_focus,
           CASE
               WHEN COALESCE(f.is_equity, 0) = 1 AND COALESCE(f.is_debt, 0) = 1 THEN 'Equity & Debt'
               WHEN COALESCE(f.is_equity, 0) = 1 THEN 'Equity'
               WHEN COALESCE(f.is_debt, 0) = 1 THEN 'Debt'
               WHEN COALESCE(f.is_pooled, 0) = 1 THEN 'Pooled Vehicle'
               ELSE 'Other'
           END AS security_type,
           COALESCE(f.allows_non_accredited, 0) AS retail_allowed,
           COALESCE(f.is_pooled, 0) AS pooled_focus,
           NULL AS unit_price,
           NULL AS employee_band
    FROM vw_fd_features f
    JOIN vw_fd_latest_offering o USING (ACCESSIONNUMBER)

    UNION ALL

    SELECT 'REG_CF' AS deal_type,
           'CF:' || f.ACCESSION_NUMBER AS deal_id,
           f.ACCESSION_NUMBER AS accession_id,
           f.NAMEOFISSUER AS issuer_name,
           UPPER(TRIM(f.STATEORCOUNTRY)) AS issuer_state,
           f.target_raise,
           f.price_bucket AS ticket_hint,
           f.SECURITYOFFEREDTYPE AS industry_focus,
           f.SECURITYOFFEREDTYPE AS security_type,
           1 AS retail_allowed,
           0 AS pooled_focus,
           f.unit_price,
           f.employee_band
    FROM vw_cf_features f
),
adv AS (
    SELECT FilingID,
           adviser_name,
           UPPER(TRIM(state)) AS hq_state,
           total_raum,
           raum_bucket,
           COALESCE(non_hnw_clients, 0) AS non_hnw_clients,
           COALESCE(hnw_clients, 0) AS hnw_clients,
           COALESCE(pooled_clients, 0) AS pooled_clients,
           registered_states,
           UPPER(REPLACE(IFNULL(registered_states, ''), ' ', '')) AS state_list_normalized
    FROM vw_adv_features
),
adv_ready AS (
    SELECT *,
           CASE
               WHEN state_list_normalized = '' THEN ''
               ELSE ',' || state_list_normalized || ','
           END AS state_list_search
    FROM adv
),
matches AS (
    SELECT d.deal_type,
           d.deal_id,
           d.accession_id,
           d.issuer_name,
           d.issuer_state,
           d.target_raise,
           d.ticket_hint,
           d.industry_focus,
           d.security_type,
           d.retail_allowed,
           d.pooled_focus,
           d.unit_price,
           d.employee_band,
           a.FilingID AS adviser_id,
           a.adviser_name,
           a.hq_state,
           a.total_raum,
           a.raum_bucket,
           a.non_hnw_clients,
           a.hnw_clients,
           a.pooled_clients,
           a.registered_states,
           CASE
               WHEN d.issuer_state IS NULL OR d.issuer_state = '' THEN 0
               WHEN a.hq_state = d.issuer_state THEN 1
               WHEN a.state_list_search <> '' AND instr(a.state_list_search, ',' || d.issuer_state || ',') > 0 THEN 1
               ELSE 0
           END AS state_match,
           CASE
               WHEN d.target_raise IS NULL OR a.total_raum IS NULL THEN 0
               WHEN d.target_raise <= a.total_raum * 0.10 THEN 1
               WHEN d.target_raise <= 2500000 THEN 1
               ELSE 0
           END AS capital_fit,
           CASE
               WHEN d.pooled_focus = 1 THEN CASE WHEN a.pooled_clients > 0 THEN 1 ELSE 0 END
               WHEN d.retail_allowed = 1 THEN CASE WHEN a.non_hnw_clients > 0 THEN 1 ELSE 0 END
               ELSE CASE WHEN a.hnw_clients > 0 THEN 1 ELSE 0 END
           END AS audience_fit
    FROM deal_universe d
    JOIN adv_ready a
      ON (
          d.issuer_state IS NULL OR
          d.issuer_state = '' OR
          a.hq_state = d.issuer_state OR
          (a.state_list_search <> '' AND instr(a.state_list_search, ',' || d.issuer_state || ',') > 0)
      )
)
SELECT *,
       state_match + capital_fit + audience_fit AS fit_score
FROM matches;

DROP VIEW IF EXISTS vw_investor_deal_scored;
CREATE VIEW vw_investor_deal_scored AS
WITH base AS (
    SELECT c.*,
           adv.adviser_name,
           adv.city AS adviser_city,
           adv.state AS adviser_state,
           adv.total_raum,
           adv.non_hnw_clients,
           adv.hnw_clients,
           adv.pooled_clients,
           adv.is_broker_dealer,
           adv.is_bank_affiliate,
           adv.is_insurance_affiliate,
           fd.min_invest,
           fd.sold_vs_target,
           fd.is_equity,
           fd.is_debt,
           fd.is_pooled,
           fd.allows_non_accredited,
           cf.unit_price,
           cf.target_vs_cap
    FROM vw_investor_deal_candidates c
    JOIN vw_adv_features adv ON adv.FilingID = c.adviser_id
    LEFT JOIN vw_fd_features fd
           ON fd.ACCESSIONNUMBER = c.accession_id
          AND c.deal_type = 'FORM_D'
    LEFT JOIN vw_cf_features cf
           ON cf.ACCESSION_NUMBER = c.accession_id
          AND c.deal_type = 'REG_CF'
)
SELECT base.*,
       CAST(
           CASE
               WHEN base.issuer_state IS NULL OR base.issuer_state = '' THEN 0.5
               WHEN base.state_match = 1 THEN 1.0
               ELSE 0.0
           END AS REAL
       ) AS geography_score,
       CAST(
           CASE
               WHEN base.target_raise IS NULL OR base.target_raise <= 0 THEN 0.5
               WHEN base.total_raum IS NULL OR base.total_raum <= 0 THEN 0.4
               ELSE CASE
                   WHEN base.target_raise / base.total_raum <= 0.01 THEN 1.0
                   WHEN base.target_raise / base.total_raum <= 0.05 THEN 0.85
                   WHEN base.target_raise / base.total_raum <= 0.1 THEN 0.7
                   WHEN base.target_raise / base.total_raum <= 0.2 THEN 0.5
                   WHEN base.target_raise / base.total_raum <= 0.5 THEN 0.3
                   ELSE 0.15
               END
           END AS REAL
       ) AS capital_score,
       CAST(
           CASE
               WHEN base.pooled_focus = 1 THEN CASE
                   WHEN COALESCE(base.pooled_clients, 0) >= 100 THEN 1.0
                   WHEN COALESCE(base.pooled_clients, 0) >= 50 THEN 0.85
                   WHEN COALESCE(base.pooled_clients, 0) >= 10 THEN 0.65
                   WHEN COALESCE(base.pooled_clients, 0) > 0 THEN 0.45
                   ELSE 0.25
               END
               WHEN base.retail_allowed = 1 THEN CASE
                   WHEN COALESCE(base.non_hnw_clients, 0) >= 100 THEN 1.0
                   WHEN COALESCE(base.non_hnw_clients, 0) >= 50 THEN 0.85
                   WHEN COALESCE(base.non_hnw_clients, 0) >= 10 THEN 0.65
                   WHEN COALESCE(base.non_hnw_clients, 0) > 0 THEN 0.45
                   ELSE 0.25
               END
               ELSE CASE
                   WHEN COALESCE(base.hnw_clients, 0) >= 100 THEN 1.0
                   WHEN COALESCE(base.hnw_clients, 0) >= 50 THEN 0.85
                   WHEN COALESCE(base.hnw_clients, 0) >= 10 THEN 0.65
                   WHEN COALESCE(base.hnw_clients, 0) > 0 THEN 0.45
                   ELSE 0.25
               END
           END AS REAL
       ) AS audience_score,
       CAST(
           CASE
               WHEN base.deal_type = 'FORM_D' THEN
                   CASE
                       WHEN base.min_invest IS NULL THEN 0.5
                       WHEN base.total_raum IS NULL OR base.total_raum <= 0 THEN 0.6
                       ELSE CASE
                           WHEN (base.min_invest / base.total_raum) <= 0.05 THEN 1.0
                           WHEN (base.min_invest / base.total_raum) <= 0.15 THEN 0.85
                           WHEN (base.min_invest / base.total_raum) <= 0.3 THEN 0.65
                           WHEN (base.min_invest / base.total_raum) <= 0.6 THEN 0.45
                           ELSE 0.25
                       END
                   END
               ELSE
                   CASE
                       WHEN base.unit_price IS NULL THEN 0.5
                       WHEN base.total_raum IS NULL OR base.total_raum <= 0 THEN 0.6
                       ELSE CASE
                           WHEN (base.unit_price / base.total_raum) <= 0.0005 THEN 1.0
                           WHEN (base.unit_price / base.total_raum) <= 0.0015 THEN 0.85
                           WHEN (base.unit_price / base.total_raum) <= 0.003 THEN 0.65
                           WHEN (base.unit_price / base.total_raum) <= 0.006 THEN 0.45
                           ELSE 0.25
                       END
                   END
           END AS REAL
       ) AS ticket_score,
       CAST(
           CASE
               WHEN base.deal_type = 'FORM_D' THEN
                   CASE
                       WHEN base.sold_vs_target IS NULL THEN 0.5
                       WHEN base.sold_vs_target >= 1.0 THEN 1.0
                       WHEN base.sold_vs_target >= 0.75 THEN 0.8
                       WHEN base.sold_vs_target >= 0.5 THEN 0.6
                       WHEN base.sold_vs_target >= 0.25 THEN 0.4
                       ELSE 0.2
                   END
               ELSE
                   CASE
                       WHEN base.target_vs_cap IS NULL THEN 0.5
                       WHEN base.target_vs_cap <= 0.8 THEN 0.6
                       ELSE 0.4
                   END
           END AS REAL
       ) AS traction_score,
       CAST(
           CASE
               WHEN base.pooled_focus = 1 THEN CASE
                   WHEN COALESCE(base.pooled_clients, 0) > 0 THEN 0.9
                   ELSE 0.4
               END
               WHEN base.security_type LIKE 'Debt%' THEN CASE
                   WHEN base.is_bank_affiliate = 1 OR base.is_broker_dealer = 1 OR base.is_insurance_affiliate = 1 THEN 0.85
                   ELSE 0.4
               END
               WHEN base.security_type LIKE 'Equity%' THEN CASE
                   WHEN COALESCE(base.hnw_clients, 0) > 0 THEN 0.8
                   ELSE 0.5
               END
               ELSE 0.5
           END AS REAL
       ) AS security_score,
       CAST(
           0.25 * (
               CASE
                   WHEN base.issuer_state IS NULL OR base.issuer_state = '' THEN 0.5
                   WHEN base.state_match = 1 THEN 1.0
                   ELSE 0.0
               END
           )
           + 0.25 * (
               CASE
                   WHEN base.target_raise IS NULL OR base.target_raise <= 0 THEN 0.5
                   WHEN base.total_raum IS NULL OR base.total_raum <= 0 THEN 0.4
                   ELSE CASE
                       WHEN base.target_raise / base.total_raum <= 0.01 THEN 1.0
                       WHEN base.target_raise / base.total_raum <= 0.05 THEN 0.85
                       WHEN base.target_raise / base.total_raum <= 0.1 THEN 0.7
                       WHEN base.target_raise / base.total_raum <= 0.2 THEN 0.5
                       WHEN base.target_raise / base.total_raum <= 0.5 THEN 0.3
                       ELSE 0.15
                   END
               END
           )
           + 0.2 * (
               CASE
                   WHEN base.pooled_focus = 1 THEN CASE
                       WHEN COALESCE(base.pooled_clients, 0) >= 100 THEN 1.0
                       WHEN COALESCE(base.pooled_clients, 0) >= 50 THEN 0.85
                       WHEN COALESCE(base.pooled_clients, 0) >= 10 THEN 0.65
                       WHEN COALESCE(base.pooled_clients, 0) > 0 THEN 0.45
                       ELSE 0.25
                   END
                   WHEN base.retail_allowed = 1 THEN CASE
                       WHEN COALESCE(base.non_hnw_clients, 0) >= 100 THEN 1.0
                       WHEN COALESCE(base.non_hnw_clients, 0) >= 50 THEN 0.85
                       WHEN COALESCE(base.non_hnw_clients, 0) >= 10 THEN 0.65
                       WHEN COALESCE(base.non_hnw_clients, 0) > 0 THEN 0.45
                       ELSE 0.25
                   END
                   ELSE CASE
                       WHEN COALESCE(base.hnw_clients, 0) >= 100 THEN 1.0
                       WHEN COALESCE(base.hnw_clients, 0) >= 50 THEN 0.85
                       WHEN COALESCE(base.hnw_clients, 0) >= 10 THEN 0.65
                       WHEN COALESCE(base.hnw_clients, 0) > 0 THEN 0.45
                       ELSE 0.25
                   END
               END
           )
           + 0.15 * (
               CASE
                   WHEN base.deal_type = 'FORM_D' THEN
                       CASE
                           WHEN base.min_invest IS NULL THEN 0.5
                           WHEN base.total_raum IS NULL OR base.total_raum <= 0 THEN 0.6
                           ELSE CASE
                               WHEN (base.min_invest / base.total_raum) <= 0.05 THEN 1.0
                               WHEN (base.min_invest / base.total_raum) <= 0.15 THEN 0.85
                               WHEN (base.min_invest / base.total_raum) <= 0.3 THEN 0.65
                               WHEN (base.min_invest / base.total_raum) <= 0.6 THEN 0.45
                               ELSE 0.25
                           END
                       END
                   ELSE
                       CASE
                           WHEN base.unit_price IS NULL THEN 0.5
                           WHEN base.total_raum IS NULL OR base.total_raum <= 0 THEN 0.6
                           ELSE CASE
                               WHEN (base.unit_price / base.total_raum) <= 0.0005 THEN 1.0
                               WHEN (base.unit_price / base.total_raum) <= 0.0015 THEN 0.85
                               WHEN (base.unit_price / base.total_raum) <= 0.003 THEN 0.65
                               WHEN (base.unit_price / base.total_raum) <= 0.006 THEN 0.45
                               ELSE 0.25
                           END
                       END
               END
           )
           + 0.1 * (
               CASE
                   WHEN base.deal_type = 'FORM_D' THEN
                       CASE
                           WHEN base.sold_vs_target IS NULL THEN 0.5
                           WHEN base.sold_vs_target >= 1.0 THEN 1.0
                           WHEN base.sold_vs_target >= 0.75 THEN 0.8
                           WHEN base.sold_vs_target >= 0.5 THEN 0.6
                           WHEN base.sold_vs_target >= 0.25 THEN 0.4
                           ELSE 0.2
                       END
                   ELSE
                       CASE
                           WHEN base.target_vs_cap IS NULL THEN 0.5
                           WHEN base.target_vs_cap <= 0.8 THEN 0.6
                           ELSE 0.4
                       END
               END
           )
           + 0.05 * (
               CASE
                   WHEN base.pooled_focus = 1 THEN CASE
                       WHEN COALESCE(base.pooled_clients, 0) > 0 THEN 0.9
                       ELSE 0.4
                   END
                   WHEN base.security_type LIKE 'Debt%' THEN CASE
                       WHEN base.is_bank_affiliate = 1 OR base.is_broker_dealer = 1 OR base.is_insurance_affiliate = 1 THEN 0.85
                       ELSE 0.4
                   END
                   WHEN base.security_type LIKE 'Equity%' THEN CASE
                       WHEN COALESCE(base.hnw_clients, 0) > 0 THEN 0.8
                       ELSE 0.5
                   END
                   ELSE 0.5
               END
           )
       AS REAL) AS composite_score
FROM base;
