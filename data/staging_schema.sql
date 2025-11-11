-- Form D (Reg D) quarterly flat file: anchor for private placement offerings.
CREATE TABLE IF NOT EXISTS stg_fd_FORMDSUBMISSION (
  ACCESSIONNUMBER text,            -- From FORMDSUBMISSION.tsv; SEC filing key to join all Form D tables.
  SUBMISSIONTYPE text,             -- FORMDSUBMISSION.tsv; tells us if filing is original D or amendment D/A (latest state tracking).
  FILING_DATE date,                -- FORMDSUBMISSION.tsv; timestamp used to pick most recent disclosure.
  FILE_NUM text,                   -- FORMDSUBMISSION.tsv; SEC file number for audit trail.
  SIC_CODE text,                   -- FORMDSUBMISSION.tsv; industry classification useful for sector tagging.
  TESTORLIVE text                  -- FORMDSUBMISSION.tsv; distinguishes live vs test submissions (filter noise).
);

CREATE TABLE IF NOT EXISTS stg_fd_ISSUERS (
  ACCESSIONNUMBER text,            -- ISSUERS.tsv; join back to submission and offering data.
  CIK text,                        -- ISSUERS.tsv; company identifier for cross-referencing other SEC filings.
  ENTITYNAME text,                 -- ISSUERS.tsv; issuer legal name for reporting and deduping.
  STREET1 text,                    -- ISSUERS.tsv; primary street address for issuer outreach.
  STREET2 text,                    -- ISSUERS.tsv; secondary street line (suite/floor).
  ENTITYTYPE text,                 -- ISSUERS.tsv; LLC/LP/etc helps infer investor target segment.
  CITY text,                       -- ISSUERS.tsv; issuer city for geo filters.
  STATEORCOUNTRY text,             -- ISSUERS.tsv; abbreviated location for matching to investor jurisdictions.
  STATEORCOUNTRYDESCRIPTION text,  -- ISSUERS.tsv; human-readable location for emails/dashboards.
  ZIPCODE text,                    -- ISSUERS.tsv; postal code for mailers or geo matching.
  JURISDICTIONOFINC text,          -- ISSUERS.tsv; incorporation jurisdiction used in compliance checks.
  YEAROFINC_VALUE_ENTERED text,    -- ISSUERS.tsv; issuer age proxy (new vs seasoned).
  ISSUERPHONENUMBER text           -- ISSUERS.tsv; optional contact detail when enriching outreach.
);

CREATE TABLE IF NOT EXISTS stg_fd_OFFERING (
  ACCESSIONNUMBER text,                -- OFFERING.tsv; join to submission/issuer.
  INDUSTRYGROUPTYPE text,              -- OFFERING.tsv; primary sector tag for investor matching.
  FEDERALEXEMPTIONS_ITEMS_LIST text,   -- OFFERING.tsv; exemption codes (e.g., 506(b)/3C.1) vital for compliance routing.
  ISEQUITYTYPE boolean,                -- OFFERING.tsv; quick security classification (equity vs debt).
  ISDEBTTYPE boolean,                  -- OFFERING.tsv; identify credit opportunities.
  ISPOOLEDINVESTMENTFUNDTYPE boolean,  -- OFFERING.tsv; flag pooled funds for institutional-only outreach.
  HASNONACCREDITEDINVESTORS boolean,   -- OFFERING.tsv; shows retail eligibility.
  TOTALOFFERINGAMOUNT text,            -- OFFERING.tsv; stated raise target for sizing.
  TOTALAMOUNTSOLD text,                -- OFFERING.tsv; progress toward raise goal (traction signal).
  TOTALREMAINING text,                 -- OFFERING.tsv; remaining capacity indicator.
  MINIMUMINVESTMENTACCEPTED text,      -- OFFERING.tsv; minimum check size critical for investor fit.
  SALE_DATE date                       -- OFFERING.tsv; most recent sale date (freshness cue).
);
-- RECIPIENTS, RELATEDPERSONS, SIGNATURES optional for v1

-- Reg CF — 2025Q1_cf/
CREATE TABLE IF NOT EXISTS stg_cf_FORM_C_SUBMISSION (
  ACCESSION_NUMBER text,  -- FORM_C_SUBMISSION.tsv; filing key across all Reg CF tables.
  SUBMISSION_TYPE text,   -- FORM_C_SUBMISSION.tsv; differentiates initial C, amendments C/A, updates C-U.
  FILING_DATE date,       -- FORM_C_SUBMISSION.tsv; latest update timestamp.
  CIK text,               -- FORM_C_SUBMISSION.tsv; issuer identifier.
  FILE_NUMBER text,       -- FORM_C_SUBMISSION.tsv; SEC file number for traceability.
  PERIOD text             -- FORM_C_SUBMISSION.tsv; reporting period context.
);

CREATE TABLE IF NOT EXISTS stg_cf_FORM_C_ISSUER_INFORMATION (
  ACCESSION_NUMBER text,         -- FORM_C_ISSUER_INFORMATION.tsv; join key.
  NAMEOFISSUER text,             -- FORM_C_ISSUER_INFORMATION.tsv; issuer name for messaging.
  LEGALSTATUSFORM text,          -- FORM_C_ISSUER_INFORMATION.tsv; LLC/corp etc informs maturity.
  JURISDICTIONORGANIZATION text, -- FORM_C_ISSUER_INFORMATION.tsv; state of incorporation.
  STREET1 text,                  -- FORM_C_ISSUER_INFORMATION.tsv; primary street address.
  STREET2 text,                  -- FORM_C_ISSUER_INFORMATION.tsv; secondary address information.
  CITY text,                     -- FORM_C_ISSUER_INFORMATION.tsv; location for geo matching.
  STATEORCOUNTRY text,           -- FORM_C_ISSUER_INFORMATION.tsv; region used in investor filters.
  ZIPCODE text,                  -- FORM_C_ISSUER_INFORMATION.tsv; postal code for outreach.
  ISSUERWEBSITE text,            -- FORM_C_ISSUER_INFORMATION.tsv; link for education emails.
  PROGRESSUPDATE text            -- FORM_C_ISSUER_INFORMATION.tsv; issuer narrative (useful copy and status signal).
);

CREATE TABLE IF NOT EXISTS stg_cf_FORM_C_DISCLOSURE (
  ACCESSION_NUMBER text,                   -- FORM_C_DISCLOSURE.tsv; join key.
  SECURITYOFFEREDTYPE text,                -- FORM_C_DISCLOSURE.tsv; security flavor (common, SAFE, debt).
  NOOFSECURITYOFFERED text,                -- FORM_C_DISCLOSURE.tsv; supply size for per-unit context.
  PRICE text,                              -- FORM_C_DISCLOSURE.tsv; unit price used for stage signaling.
  OFFERINGAMOUNT text,                     -- FORM_C_DISCLOSURE.tsv; target raise amount.
  MAXIMUMOFFERINGAMOUNT text,              -- FORM_C_DISCLOSURE.tsv; cap on raise (oversubscription limit).
  OVERSUBSCRIPTIONACCEPTED text,           -- FORM_C_DISCLOSURE.tsv; indicates if extra funds can be taken.
  OVERSUBSCRIPTIONALLOCATIONTYPE text,     -- FORM_C_DISCLOSURE.tsv; rules for allocating excess demand.
  DEADLINEDATE date,                       -- FORM_C_DISCLOSURE.tsv; closing deadline to prioritize outreach.
  CURRENTEMPLOYEES text,                   -- FORM_C_DISCLOSURE.tsv; team size proxy for maturity.
  TOTALASSETMOSTRECENTFISCALYEAR text,     -- FORM_C_DISCLOSURE.tsv; financial footing metric.
  REVENUEMOSTRECENTFISCALYEAR text,        -- FORM_C_DISCLOSURE.tsv; traction metric.
  NETINCOMEMOSTRECENTFISCALYEAR text,      -- FORM_C_DISCLOSURE.tsv; profitability insight.
  TOTALASSETPRIORYEAR text,                -- FORM_C_DISCLOSURE.tsv; YoY comparison.
  REVENUEPRIORYEAR text,                   -- FORM_C_DISCLOSURE.tsv; revenue trend.
  NETINCOMEPRIORYEAR text                  -- FORM_C_DISCLOSURE.tsv; net income trend (growth vs burn).
);

CREATE TABLE IF NOT EXISTS stg_cf_FORM_C_ISSUER_JURISDICTIONS (
  ACCESSION_NUMBER text,  -- FORM_C_ISSUER_JURISDICTIONS.tsv; join key.
  STATEORPROVINCE text,   -- FORM_C_ISSUER_JURISDICTIONS.tsv; allowed investor jurisdictions (compliance gating).
  COUNTRY text            -- FORM_C_ISSUER_JURISDICTIONS.tsv; country context (default blank in flat file).
);

-- Form ADV — Part 1 (subset)
CREATE TABLE IF NOT EXISTS stg_adv_base_a (
  FilingID text,        -- IA_ADV_Base_A.csv; adviser filing key across ADV tables.
  DateSubmitted date,   -- IA_ADV_Base_A.csv; latest submission date for recency filters.
  "1A" text,            -- IA_ADV_Base_A.csv; adviser legal name.
  "1F1-Street 1" text,  -- IA_ADV_Base_A.csv; main office street address line 1.
  "1F1-Street 2" text,  -- IA_ADV_Base_A.csv; main office street address line 2.
  "1F1-City" text,      -- IA_ADV_Base_A.csv; main office city (geo matching).
  "1F1-State" text,     -- IA_ADV_Base_A.csv; main office state (licensing context).
  "1F1-Country" text,   -- IA_ADV_Base_A.csv; country indicator.
  "1F1-Postal" text,    -- IA_ADV_Base_A.csv; main office postal code.
  "1F1-Private" text,   -- IA_ADV_Base_A.csv; indicates if main office address is private.
  "1F2-M-F" text,       -- IA_ADV_Base_A.csv; main office Monday-Friday hours.
  "1F2-Other" text,     -- IA_ADV_Base_A.csv; alternate office hours description.
  "1F2-Hours" text,     -- IA_ADV_Base_A.csv; overall office hours narrative.
  "1F3" text,           -- IA_ADV_Base_A.csv; main office telephone number.
  "1F4" text,           -- IA_ADV_Base_A.csv; main office fax number.
  "1F5" text,           -- IA_ADV_Base_A.csv; adviser website.
  "1G-Street 1" text,   -- IA_ADV_Base_A.csv; mailing address street line 1.
  "1G-Street 2" text,   -- IA_ADV_Base_A.csv; mailing address street line 2.
  "1G-City" text,       -- IA_ADV_Base_A.csv; mailing address city.
  "1G-State" text,      -- IA_ADV_Base_A.csv; mailing address state.
  "1G-Country" text,    -- IA_ADV_Base_A.csv; mailing address country.
  "1G-Postal" text,     -- IA_ADV_Base_A.csv; mailing address postal code.
  "1G-Private" text,    -- IA_ADV_Base_A.csv; indicates if mailing address is private.
  "5F2a" numeric,       -- IA_ADV_Base_A.csv; discretionary RAUM (AUM sizing).
  "5F2b" numeric,       -- IA_ADV_Base_A.csv; non-discretionary RAUM breakdown.
  "5F2c" numeric,       -- IA_ADV_Base_A.csv; total RAUM overall.
  "5D1a" integer,       -- IA_ADV_Base_A.csv; count of non-high-net-worth clients.
  "5D1b" integer,       -- IA_ADV_Base_A.csv; count of high-net-worth clients.
  "5D1c" integer,       -- IA_ADV_Base_A.csv; count of pooled investment vehicle clients.
  "5D1e" integer,       -- IA_ADV_Base_A.csv; count of investment company clients (private funds).
  "5D1f" integer,       -- IA_ADV_Base_A.csv; count of pension/profit-sharing plan clients.
  "5D1n" integer,       -- IA_ADV_Base_A.csv; total clients (sanity check).
  "5D2a" numeric,       -- IA_ADV_Base_A.csv; RAUM attributed to non-HNW clients (when reported).
  "5D2b" numeric,       -- IA_ADV_Base_A.csv; RAUM for HNW clients.
  "5D2c" numeric,       -- IA_ADV_Base_A.csv; RAUM for pooled vehicles.
  "5D2g" numeric,       -- IA_ADV_Base_A.csv; RAUM for pension plans.
  "5D2h" numeric,       -- IA_ADV_Base_A.csv; RAUM for charities.
  "5D2j" numeric,       -- IA_ADV_Base_A.csv; RAUM for corporations.
  "5D2k" numeric,       -- IA_ADV_Base_A.csv; RAUM for other clients (catch-all).
  "5H" text,            -- IA_ADV_Base_A.csv; wrap-fee program scale (distribution reach).
  "5J2" text,           -- IA_ADV_Base_A.csv; performance fee flag (risk appetite).
  "5K1" boolean,        -- IA_ADV_Base_A.csv; broker-dealer affiliation indicator.
  "5K2" boolean,        -- IA_ADV_Base_A.csv; bank/thrift affiliation indicator.
  "5K3" boolean,        -- IA_ADV_Base_A.csv; insurance affiliation indicator.
  "5K4" boolean,        -- IA_ADV_Base_A.csv; other financial affiliation (potential conflicts).
  "7A1" text,           -- IA_ADV_Base_A.csv; if adviser is a broker-dealer (compliance routing).
  "7A2" text,           -- IA_ADV_Base_A.csv; if adviser is a commodity pool operator/trading adviser.
  "7A3" text,           -- IA_ADV_Base_A.csv; investment company (mutual fund) affiliation.
  "7A6" text,           -- IA_ADV_Base_A.csv; banking/thrift affiliations beyond 5K flags.
  "7A8" text,           -- IA_ADV_Base_A.csv; insurance company relationships.
  "7A9" text,           -- IA_ADV_Base_A.csv; other financial institutions ties.
  "7A10" text,          -- IA_ADV_Base_A.csv; sponsor of wrap fee program indicator.
  "7A12" text,          -- IA_ADV_Base_A.csv; real estate broker/dealer or commodity relationships.
  "7A16" text,          -- IA_ADV_Base_A.csv; sponsor of investment company/BD (extra compliance).
  "9A1a" boolean,       -- IA_ADV_Base_A.csv; custody of client cash/securities (Y/N).
  "9A1b" boolean,       -- IA_ADV_Base_A.csv; custody due to related person.
  "9A2a" numeric,       -- IA_ADV_Base_A.csv; total client assets held in custody.
  "9A2b" numeric        -- IA_ADV_Base_A.csv; discretionary custody amounts (if different).
);

CREATE TABLE IF NOT EXISTS stg_adv_base_b (
  FilingID text,          -- IA_ADV_Base_B.csv; join back to base_a (same FilingID).
  "2-SECStateReg" text,   -- IA_ADV_Base_B.csv; comma list of states where adviser is registered (from 2-XX flags).
  "3A" text,              -- IA_ADV_Base_B.csv; legal form of organization (LLC, corp, etc.).
  "3A-Other" text         -- IA_ADV_Base_B.csv; free-form description when 3A = Other.
);
