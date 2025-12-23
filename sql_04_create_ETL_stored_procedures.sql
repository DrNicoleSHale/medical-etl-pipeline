-- ============================================================================
-- ETL STORED PROCEDURES
-- ============================================================================
-- PURPOSE: Transform source data into analytics-ready tables.
--          Each procedure handles one target table with full reload logic.
--
-- PATTERN: TRUNCATE and reload (not incremental)
--          Simple, reliable, appropriate for daily batch processing
-- ============================================================================

-- ============================================================================
-- PROCEDURE 1: Load Medical Encounters (Base Fact Table)
-- ============================================================================
-- Combines data from 5 source tables into one denormalized fact table.
-- Key techniques: Multi-table JOINs, CASE statements, date calculations
-- ============================================================================

CREATE OR ALTER PROCEDURE usp_load_medical_encounters
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Full reload pattern: clear and repopulate
    TRUNCATE TABLE analytics.medical_encounters;
    
    INSERT INTO analytics.medical_encounters (
        event_id,
        patient_id,
        physician_id,
        physician_name,
        specialty,
        admit_date,
        discharge_date,
        length_of_stay,
        admission_type,
        is_emergency,
        discharge_type,
        patient_age,
        age_group,
        total_cost
    )
    SELECT 
        e.event_id,
        e.patient_id,
        e.physician_id,
        
        -- Denormalize physician name (handle NULLs gracefully)
        CASE 
            WHEN p.physician_id IS NOT NULL 
            THEN CONCAT(p.first_name, ' ', p.last_name)
            ELSE 'Unassigned'
        END AS physician_name,
        
        COALESCE(p.specialty, 'Unknown') AS specialty,
        
        e.admit_date,
        e.discharge_date,
        
        -- Calculate length of stay (NULL if still admitted)
        DATEDIFF(DAY, e.admit_date, e.discharge_date) AS length_of_stay,
        
        COALESCE(at.admission_type_desc, 'Unknown') AS admission_type,
        COALESCE(at.is_emergency, 0) AS is_emergency,
        COALESCE(dt.discharge_type_desc, 'Not Discharged') AS discharge_type,
        
        -- Calculate age at time of admission
        DATEDIFF(YEAR, pat.date_of_birth, e.admit_date) AS patient_age,
        
        -- Classify into age groups for reporting
        CASE 
            WHEN DATEDIFF(YEAR, pat.date_of_birth, e.admit_date) < 18 THEN '0-17'
            WHEN DATEDIFF(YEAR, pat.date_of_birth, e.admit_date) < 45 THEN '18-44'
            WHEN DATEDIFF(YEAR, pat.date_of_birth, e.admit_date) < 65 THEN '45-64'
            ELSE '65+'
        END AS age_group,
        
        e.total_cost
        
    FROM source.medical_events e
    
    -- LEFT JOINs preserve events even with missing reference data
    -- This is critical: we don't want to lose encounters just because
    -- a physician wasn't properly assigned
    LEFT JOIN source.physicians p ON e.physician_id = p.physician_id
    LEFT JOIN source.patients pat ON e.patient_id = pat.patient_id
    LEFT JOIN source.admission_types at ON e.admission_type_id = at.admission_type_id
    LEFT JOIN source.discharge_types dt ON e.discharge_type_id = dt.discharge_type_id;
    
    PRINT CONCAT('Loaded ', @@ROWCOUNT, ' rows into analytics.medical_encounters');
END;
GO


-- ============================================================================
-- PROCEDURE 2: Load Cost Summary (Aggregations)
-- ============================================================================
-- Pre-aggregates costs by period, admission type, and specialty.
-- Key techniques: GROUP BY, aggregate functions, date truncation
-- ============================================================================

CREATE OR ALTER PROCEDURE usp_load_cost_summary
AS
BEGIN
    SET NOCOUNT ON;
    
    TRUNCATE TABLE analytics.cost_summary;
    
    INSERT INTO analytics.cost_summary (
        summary_period,
        admission_type,
        specialty,
        encounter_count,
        total_cost,
        avg_cost,
        min_cost,
        max_cost,
        avg_length_of_stay
    )
    SELECT 
        -- Truncate to first of month for period grouping
        DATEFROMPARTS(YEAR(admit_date), MONTH(admit_date), 1) AS summary_period,
        admission_type,
        specialty,
        COUNT(*) AS encounter_count,
        SUM(total_cost) AS total_cost,
        ROUND(AVG(total_cost), 2) AS avg_cost,
        MIN(total_cost) AS min_cost,
        MAX(total_cost) AS max_cost,
        ROUND(AVG(CAST(length_of_stay AS DECIMAL(5,1))), 1) AS avg_length_of_stay
    FROM analytics.medical_encounters
    WHERE total_cost IS NOT NULL  -- Exclude still-admitted patients
    GROUP BY 
        DATEFROMPARTS(YEAR(admit_date), MONTH(admit_date), 1),
        admission_type,
        specialty;
    
    PRINT CONCAT('Loaded ', @@ROWCOUNT, ' rows into analytics.cost_summary');
END;
GO


-- ============================================================================
-- PROCEDURE 3: Load First Visits (Window Functions)
-- ============================================================================
-- Identifies each patient's first encounter for cohort analysis.
-- Key techniques: ROW_NUMBER(), PARTITION BY, CTE
-- ============================================================================

CREATE OR ALTER PROCEDURE usp_load_first_visits
AS
BEGIN
    SET NOCOUNT ON;
    
    TRUNCATE TABLE analytics.first_visits;
    
    -- Use CTE with ROW_NUMBER to find first visit per patient
    WITH ranked_visits AS (
        SELECT 
            patient_id,
            admit_date,
            physician_id,
            physician_name,
            specialty,
            admission_type,
            -- Rank visits by date, earliest first
            ROW_NUMBER() OVER (
                PARTITION BY patient_id 
                ORDER BY admit_date ASC
            ) AS visit_rank
        FROM analytics.medical_encounters
    )
    INSERT INTO analytics.first_visits (
        patient_id,
        first_visit_date,
        first_physician_id,
        first_physician,
        first_specialty,
        first_admission_type
    )
    SELECT 
        patient_id,
        admit_date,
        physician_id,
        physician_name,
        specialty,
        admission_type
    FROM ranked_visits
    WHERE visit_rank = 1;  -- Keep only the first visit
    
    PRINT CONCAT('Loaded ', @@ROWCOUNT, ' rows into analytics.first_visits');
END;
GO


-- ============================================================================
-- PROCEDURE 4: Load Readmissions (Self-Join Logic)
-- ============================================================================
-- Identifies patients readmitted within 30 days (CMS quality metric).
-- Key techniques: Self-join, DATEDIFF, business rule implementation
-- ============================================================================

CREATE OR ALTER PROCEDURE usp_load_readmissions
AS
BEGIN
    SET NOCOUNT ON;
    
    TRUNCATE TABLE analytics.readmission_analysis;
    
    -- Self-join to find readmissions
    -- Join each discharge to any subsequent admission for same patient
    INSERT INTO analytics.readmission_analysis (
        patient_id,
        initial_event_id,
        initial_admit_date,
        initial_discharge,
        initial_diagnosis,
        readmit_event_id,
        readmit_date,
        readmit_diagnosis,
        days_to_readmit,
        is_30_day_readmit,
        is_7_day_readmit
    )
    SELECT 
        initial.patient_id,
        initial.event_id AS initial_event_id,
        initial.admit_date AS initial_admit_date,
        initial.discharge_date AS initial_discharge,
        me_init.diagnosis_code AS initial_diagnosis,
        
        readmit.event_id AS readmit_event_id,
        readmit.admit_date AS readmit_date,
        me_readmit.diagnosis_code AS readmit_diagnosis,
        
        DATEDIFF(DAY, initial.discharge_date, readmit.admit_date) AS days_to_readmit,
        
        -- CMS 30-day readmission flag
        CASE WHEN DATEDIFF(DAY, initial.discharge_date, readmit.admit_date) <= 30 
             THEN 1 ELSE 0 END AS is_30_day_readmit,
             
        -- Internal 7-day flag (more urgent quality concern)
        CASE WHEN DATEDIFF(DAY, initial.discharge_date, readmit.admit_date) <= 7 
             THEN 1 ELSE 0 END AS is_7_day_readmit
             
    FROM analytics.medical_encounters initial
    
    -- Self-join: find the NEXT admission for the same patient
    INNER JOIN analytics.medical_encounters readmit
        ON initial.patient_id = readmit.patient_id
        AND readmit.admit_date > initial.discharge_date  -- Must be after discharge
        AND DATEDIFF(DAY, initial.discharge_date, readmit.admit_date) <= 30  -- Within 30 days
        
    -- Get diagnosis codes from source
    LEFT JOIN source.medical_events me_init ON initial.event_id = me_init.event_id
    LEFT JOIN source.medical_events me_readmit ON readmit.event_id = me_readmit.event_id
    
    WHERE initial.discharge_date IS NOT NULL;  -- Must be discharged to have readmission
    
    PRINT CONCAT('Loaded ', @@ROWCOUNT, ' rows into analytics.readmission_analysis');
END;
GO


-- ============================================================================
-- PROCEDURE 5: Load Specialty Counts (PIVOT)
-- ============================================================================
-- Creates a pivoted view of encounter counts by specialty.
-- Key techniques: PIVOT operator, dynamic crosstab
-- ============================================================================

CREATE OR ALTER PROCEDURE usp_load_specialty_counts
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Clear existing data for the periods we're loading
    DELETE FROM analytics.specialty_counts 
    WHERE report_period IN (
