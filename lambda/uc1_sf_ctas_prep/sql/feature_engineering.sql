-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
-- SPDX-License-Identifier: MIT-0
-- =============================================================================
-- feature_engineering.sql
--
-- Athena CTAS query: compute patient-level ADR features from HealthLake FHIR
-- Iceberg tables and write the result to S3 in Parquet format.
--
-- Placeholders substituted by uc1_sf_ctas_prep/index.py at runtime:
--   {hl_resource_link_db}    -- Glue resource link DB auto-created by HealthLake
--   {feature_output_bucket}  -- S3 bucket for Parquet output
--   {feature_glue_db}        -- Glue database for the CTAS target table
--
-- NOTES ON SYNTHEA DATA CHARACTERISTICS:
--   1. Conditions: Synthea generates Condition resources using SNOMED CT
--      (http://snomed.info/sct) as the primary coding system. ICD-10-CM is NOT
--      added by default in bulk FHIR export mode. The comorbidity_count CTE
--      therefore filters on active clinical status only (no coding system filter).
--
--   2. Lab abnormality: Synthea does not populate the Observation.interpretation
--      field nor Observation.referenceRange in its FHIR R4 bulk export. Abnormality
--      is therefore determined by comparing valueQuantity.value against a lookup
--      table of clinical reference ranges for the LOINC codes that Synthea generates.
--      Reference ranges sourced from standard clinical laboratory references
--      (AACC, Mayo Clinic reference intervals).
-- =============================================================================

CREATE TABLE {feature_glue_db}.healthcare_features
WITH (
    format            = 'PARQUET',
    external_location = 's3://{feature_output_bucket}/features/',
    write_compression = 'SNAPPY'
)
AS
WITH patients AS (
    SELECT
        id   AS patient_uuid,
        gender,
        CAST(
            date_diff(
                'year',
                date(from_iso8601_timestamp(birthdate)),
                current_date
            ) AS INTEGER
        ) AS age_years
    FROM "{hl_resource_link_db}".patient
    WHERE id IS NOT NULL
      AND birthdate IS NOT NULL
),

med_start AS (
    SELECT
        REPLACE(subject.reference, 'Patient/', '') AS patient_uuid,
        MIN(from_iso8601_timestamp(authoredon))    AS first_med_start
    FROM "{hl_resource_link_db}".medicationrequest
    WHERE authoredon IS NOT NULL
      AND subject.reference IS NOT NULL
    GROUP BY REPLACE(subject.reference, 'Patient/', '')
),

ed_visits AS (
    SELECT
        REPLACE(e.subject.reference, 'Patient/', '') AS patient_uuid,
        COUNT(*)                                     AS ed_visit_count
    FROM "{hl_resource_link_db}".encounter e
    JOIN med_start ms
      ON REPLACE(e.subject.reference, 'Patient/', '') = ms.patient_uuid
    WHERE LOWER(COALESCE(e.class.code, TRY(e.type[1].coding[1].code), ''))
            IN ('emergency', 'emer')
      AND from_iso8601_timestamp(e.period.start) >= ms.first_med_start
      AND from_iso8601_timestamp(e.period.start) <  ms.first_med_start + INTERVAL '90' DAY
    GROUP BY REPLACE(e.subject.reference, 'Patient/', '')
),

hospitalizations AS (
    SELECT
        REPLACE(e.subject.reference, 'Patient/', '') AS patient_uuid,
        1                                            AS hospitalization_flag
    FROM "{hl_resource_link_db}".encounter e
    JOIN med_start ms
      ON REPLACE(e.subject.reference, 'Patient/', '') = ms.patient_uuid
    WHERE LOWER(COALESCE(e.class.code, TRY(e.type[1].coding[1].code), ''))
            IN ('inpatient', 'imp', 'acute')
      AND from_iso8601_timestamp(e.period.start) >= ms.first_med_start
      AND from_iso8601_timestamp(e.period.start) <  ms.first_med_start + INTERVAL '90' DAY
    GROUP BY REPLACE(e.subject.reference, 'Patient/', '')
),

-- ---------------------------------------------------------------------------
-- Lab abnormality: compare valueQuantity against clinical reference ranges.
--
-- Synthea does not populate Observation.interpretation or referenceRange, so
-- we use a reference range lookup keyed by LOINC code.
--
-- Reference ranges are standard clinical intervals from AACC and Mayo Clinic:
--   Glucose (fasting): 70-99 mg/dL
--   Creatinine:        0.5-1.2 mg/dL
--   eGFR:              >=60 mL/min/1.73m2 (>=60 = normal kidney function)
--   HbA1c:             4.0-5.6 % (>=6.5 = diabetes threshold)
--   Total Cholesterol: <200 mg/dL
--   LDL (direct):      <100 mg/dL (optimal for at-risk patients)
--   HDL:               >=40 mg/dL
--   Triglycerides:     <150 mg/dL
--   Hemoglobin:        12.0-17.5 g/dL
--   Heart rate:        60-100 /min
--   Respiratory rate:  12-20 /min
--   BMI:               18.5-24.9 kg/m2
--   Chloride:          98-107 mmol/L
--   Potassium:         3.5-5.0 mmol/L
--   CO2:               22-29 mmol/L
--   Calcium:           8.5-10.5 mg/dL
--   Chloride serum:    98-107 mmol/L
-- ---------------------------------------------------------------------------
loinc_ref_ranges AS (
    SELECT *
    FROM (
        VALUES
          -- Glucose (blood and serum)
          ('2339-0',   70.0,   99.0),
          ('2345-7',   70.0,   99.0),
          -- Sodium (blood and serum)
          ('2947-0',  135.0,  145.0),
          ('2951-2',  135.0,  145.0),
          -- Potassium (blood and serum)
          ('6298-4',    3.5,    5.0),
          ('2823-3',    3.5,    5.0),
          -- Chloride (blood and serum)
          ('2069-3',   98.0,  107.0),
          ('2075-0',   98.0,  107.0),
          -- CO2 total (blood and serum)
          ('20565-8',  22.0,   29.0),
          ('2028-9',   22.0,   29.0),
          -- Creatinine (blood and serum)
          ('38483-4',   0.5,    1.2),
          ('2160-0',    0.5,    1.2),
          -- BUN (blood and serum)
          ('6299-2',    7.0,   20.0),
          ('3094-0',    7.0,   20.0),
          -- Calcium (blood and serum)
          ('49765-1',   8.5,   10.5),
          ('17861-6',   8.5,   10.5),
          -- eGFR (>=60 = normal kidney function)
          ('33914-3',  60.0,  999.0),
          -- Hemoglobin A1c (4.0-5.6% = normal; >=6.5% = diabetes)
          ('4548-4',    4.0,    5.6),
          -- Lipid panel
          ('2093-3',    0.0,  199.0),  -- Total cholesterol (<200)
          ('2085-9',   40.0,  999.0),  -- HDL (>40 men / >50 women; 40 conservative lower bound)
          ('2571-8',    0.0,  149.0),  -- Triglycerides (<150)
          ('18262-6',   0.0,   99.0),  -- LDL direct (<100 optimal)
          -- Hematology
          ('718-7',    12.0,   17.5),  -- Hemoglobin (12-16 F, 13.5-17.5 M; wide range)
          ('787-2',    80.0,  100.0),  -- MCV
          -- Vital signs
          ('8867-4',   60.0,  100.0),  -- Heart rate
          ('9279-1',   12.0,   20.0),  -- Respiratory rate
          ('39156-5',  18.5,   29.9)   -- BMI (18.5-29.9 = normal+overweight; >30 = obese)
    ) AS t(loinc_code, ref_low, ref_high)
),

lab_abnormalities AS (
    SELECT
        REPLACE(o.subject.reference, 'Patient/', '') AS patient_uuid,
        CAST(
            MAX(
                CASE
                    WHEN o.valuequantity.value < r.ref_low
                      OR o.valuequantity.value > r.ref_high
                    THEN 1 ELSE 0
                END
            ) AS INTEGER
        ) AS lab_abnormality_flag,
        CAST(
            COUNT(
                CASE
                    WHEN o.valuequantity.value < r.ref_low
                      OR o.valuequantity.value > r.ref_high
                    THEN 1 ELSE NULL
                END
            ) AS INTEGER
        ) AS lab_severity_score
    FROM "{hl_resource_link_db}".observation o
    JOIN loinc_ref_ranges r
      ON TRY(o.code.coding[1].code) = r.loinc_code
    WHERE TRY(o.code.coding[1].system) LIKE '%loinc%'
      AND o.valuequantity.value IS NOT NULL
      AND o.subject.reference IS NOT NULL
    GROUP BY REPLACE(o.subject.reference, 'Patient/', '')
),

drug_discontinuation AS (
    SELECT
        REPLACE(subject.reference, 'Patient/', '') AS patient_uuid,
        CAST(
            MAX(CASE WHEN LOWER(status) = 'stopped' THEN 1 ELSE 0 END)
            AS INTEGER
        ) AS drug_discontinuation_flag,
        CAST(
            MIN(
                CASE
                    WHEN LOWER(status) = 'stopped'
                         AND authoredon IS NOT NULL
                         AND TRY(dispenserequest.validityperiod."end") IS NOT NULL
                    THEN date_diff(
                            'day',
                            date(from_iso8601_timestamp(authoredon)),
                            date(from_iso8601_timestamp(
                                dispenserequest.validityperiod."end"
                            ))
                         )
                    ELSE 999
                END
            ) AS INTEGER
        ) AS days_to_discontinuation
    FROM "{hl_resource_link_db}".medicationrequest
    WHERE subject.reference IS NOT NULL
    GROUP BY REPLACE(subject.reference, 'Patient/', '')
),

-- ---------------------------------------------------------------------------
-- Comorbidity count: active conditions regardless of coding system.
--
-- Synthea generates Conditions with SNOMED CT codes (http://snomed.info/sct),
-- not ICD-10-CM, in its default FHIR R4 bulk export. Filtering on coding system
-- is therefore omitted; clinical status = 'active' is the sole discriminator.
-- ---------------------------------------------------------------------------
comorbidities AS (
    SELECT
        REPLACE(c.subject.reference, 'Patient/', '') AS patient_uuid,
        CAST(COUNT(DISTINCT c.id) AS INTEGER)        AS comorbidity_count
    FROM "{hl_resource_link_db}".condition c
    WHERE LOWER(TRY(c.clinicalstatus.coding[1].code)) = 'active'
      AND c.subject.reference IS NOT NULL
    GROUP BY REPLACE(c.subject.reference, 'Patient/', '')
)

SELECT
    m.patient_token                            AS patient_id,
    COALESCE(ev.ed_visit_count,            0) AS ed_visit_count,
    COALESCE(h.hospitalization_flag,       0) AS hospitalization_flag,
    COALESCE(la.lab_abnormality_flag,      0) AS lab_abnormality_flag,
    COALESCE(la.lab_severity_score,        0) AS lab_severity_score,
    COALESCE(dd.drug_discontinuation_flag, 0) AS drug_discontinuation_flag,
    COALESCE(dd.days_to_discontinuation, 999) AS days_to_discontinuation,
    COALESCE(cm.comorbidity_count,         0) AS comorbidity_count,
    CASE
        WHEN p.age_years < 40              THEN '<40'
        WHEN p.age_years BETWEEN 40 AND 64 THEN '40-64'
        ELSE '65+'
    END                                        AS age_group,
    CASE
        WHEN LOWER(p.gender) = 'male'   THEN 'M'
        WHEN LOWER(p.gender) = 'female' THEN 'F'
        ELSE 'other'
    END                                        AS gender

FROM patients p
JOIN med_start ms
  ON p.patient_uuid = ms.patient_uuid
INNER JOIN {feature_glue_db}.patient_token_mapping m
  ON p.patient_uuid = m.patient_uuid
LEFT JOIN ed_visits            ev ON p.patient_uuid = ev.patient_uuid
LEFT JOIN hospitalizations     h  ON p.patient_uuid = h.patient_uuid
LEFT JOIN lab_abnormalities    la ON p.patient_uuid = la.patient_uuid
LEFT JOIN drug_discontinuation dd ON p.patient_uuid = dd.patient_uuid
LEFT JOIN comorbidities        cm ON p.patient_uuid = cm.patient_uuid
