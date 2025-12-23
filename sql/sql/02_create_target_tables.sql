-- ============================================================================
-- TARGET TABLE DEFINITIONS
-- ============================================================================
-- PURPOSE: Define the analytics-ready tables that the ETL populates.
--          These are optimized for reporting and analysis, not transactions.
--
-- DESIGN PHILOSOPHY:
--   - Denormalized for query performance
--   - Pre-calculated fields to avoid runtime computation
--   - Clear naming conventions for business users
-- ============================================================================

-- Create schema for analytics output
CREATE SCHEMA IF NOT EXISTS analytics;

-- ============================================================================
-- MEDICAL ENCOUNTERS: Consolidated fact table
-- One row per encounter with all dimensions joined in
-- ============================================================================
CREATE TABLE analytics.medical_encounters (
    event_id            INT PRIMARY KEY,
    patient_id          INT NOT NULL,
    physician_id        INT,
    physician_name      VARCHAR(100),           -- Denormalized for easy reporting
    specialty           VARCHAR(100),
    
    -- Dates
    admit_date          DATE NOT NULL,
    discharge_date      DATE,
    length_of_stay      INT,                    -- Pre-calculated days
    
    -- Admission/Discharge info
    admission_type      VARCHAR(100),
    is_emergency        BIT,
    discharge_type      VARCHAR(100),
    
    -- Patient demographics at time of visit
    patient_age         INT,
    age_group           VARCHAR(20),            -- '0-17', '18-44', '45-64', '65+'
    
    -- Financials
    total_cost          DECIMAL(12,2),
    
    -- Metadata
    load_timestamp      DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IX_encounters_patient ON analytics.medical_encounters(patient_id);
CREATE INDEX IX_encounters_admit ON analytics.medical_encounters(admit_date);
CREATE INDEX IX_encounters_specialty ON analytics.medical_encounters(specialty);
CREATE INDEX IX_encounters_age_group ON analytics.medical_encounters(age_group);


-- ============================================================================
-- COST SUMMARY: Aggregated costs by dimension
-- Pre-aggregated for dashboard performance
-- ============================================================================
CREATE TABLE analytics.cost_summary (
    summary_id          INT IDENTITY(1,1) PRIMARY KEY,
    summary_period      DATE NOT NULL,          -- First of month
    admission_type      VARCHAR(100),
    specialty           VARCHAR(100),
    
    -- Metrics
    encounter_count     INT,
    total_cost          DECIMAL(14,2),
    avg_cost            DECIMAL(12,2),
    min_cost            DECIMAL(12,2),
    max_cost            DECIMAL(12,2),
    avg_length_of_stay  DECIMAL(5,1),
    
    load_timestamp      DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IX_cost_summary_period ON analytics.cost_summary(summary_period);


-- ============================================================================
-- FIRST VISITS: Each patient's initial encounter
-- Useful for new patient analysis and cohort tracking
-- ============================================================================
CREATE TABLE analytics.first_visits (
    patient_id          INT PRIMARY KEY,
    first_visit_date    DATE NOT NULL,
    first_physician_id  INT,
    first_physician     VARCHAR(100),
    first_specialty     VARCHAR(100),
    first_admission_type VARCHAR(100),
    first_diagnosis     VARCHAR(10),
    
    load_timestamp      DATETIME DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================================
-- READMISSION ANALYSIS: Patients readmitted within N days
-- Critical for CMS quality metrics and penalty avoidance
-- ============================================================================
CREATE TABLE analytics.readmission_analysis (
    readmission_id      INT IDENTITY(1,1) PRIMARY KEY,
    patient_id          INT NOT NULL,
    
    -- Initial admission
    initial_event_id    INT NOT NULL,
    initial_admit_date  DATE NOT NULL,
    initial_discharge   DATE NOT NULL,
    initial_diagnosis   VARCHAR(10),
    
    -- Readmission
    readmit_event_id    INT NOT NULL,
    readmit_date        DATE NOT NULL,
    readmit_diagnosis   VARCHAR(10),
    
    -- Calculated fields
    days_to_readmit     INT NOT NULL,
    is_30_day_readmit   BIT,                    -- CMS metric
    is_7_day_readmit    BIT,                    -- Internal metric
    
    load_timestamp      DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IX_readmit_patient ON analytics.readmission_analysis(patient_id);
CREATE INDEX IX_readmit_dates ON analytics.readmission_analysis(initial_discharge, readmit_date);


-- ============================================================================
-- SPECIALTY COUNTS: Pivoted view of encounters by specialty
-- Pre-pivoted for executive dashboards
-- ============================================================================
CREATE TABLE analytics.specialty_counts (
    report_period       DATE NOT NULL,
    
    -- Each specialty becomes a column (sample specialties)
    cardiology          INT DEFAULT 0,
    orthopedics         INT DEFAULT 0,
    neurology           INT DEFAULT 0,
    oncology            INT DEFAULT 0,
    internal_medicine   INT DEFAULT 0,
    emergency_medicine  INT DEFAULT 0,
    surgery             INT DEFAULT 0,
    pediatrics          INT DEFAULT 0,
    other               INT DEFAULT 0,
    
    total_encounters    INT DEFAULT 0,
    
    load_timestamp      DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (report_period)
);
