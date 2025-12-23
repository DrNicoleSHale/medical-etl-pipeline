-- ============================================================================
-- SOURCE TABLE DEFINITIONS
-- ============================================================================
-- PURPOSE: Define the raw data tables that feed into the ETL pipeline.
--          These represent data as it arrives from operational systems.
--
-- NOTE: In production, these would be populated by upstream feeds.
--       Here we define the structure for our transformation layer to consume.
-- ============================================================================

-- Create schema for source data
CREATE SCHEMA IF NOT EXISTS source;

-- ============================================================================
-- MEDICAL EVENTS: Core transactional table
-- Each row represents a patient encounter (admission, visit, procedure)
-- ============================================================================
CREATE TABLE source.medical_events (
    event_id            INT PRIMARY KEY,
    patient_id          INT NOT NULL,
    physician_id        INT,                    -- May be NULL if not assigned
    admit_date          DATE NOT NULL,
    discharge_date      DATE,                   -- NULL if still admitted
    admission_type_id   INT,
    discharge_type_id   INT,
    total_cost          DECIMAL(12,2),
    diagnosis_code      VARCHAR(10),
    department          VARCHAR(50),
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Index for common query patterns
CREATE INDEX IX_medical_events_patient ON source.medical_events(patient_id);
CREATE INDEX IX_medical_events_dates ON source.medical_events(admit_date, discharge_date);
CREATE INDEX IX_medical_events_physician ON source.medical_events(physician_id);


-- ============================================================================
-- ADMISSION TYPES: Reference table for how patients were admitted
-- ============================================================================
CREATE TABLE source.admission_types (
    admission_type_id   INT PRIMARY KEY,
    admission_type_code VARCHAR(10) NOT NULL,
    admission_type_desc VARCHAR(100) NOT NULL,
    is_emergency        BIT DEFAULT 0,          -- Flag for ER admissions
    is_active           BIT DEFAULT 1
);


-- ============================================================================
-- DISCHARGE TYPES: Reference table for how patients left
-- ============================================================================
CREATE TABLE source.discharge_types (
    discharge_type_id   INT PRIMARY KEY,
    discharge_type_code VARCHAR(10) NOT NULL,
    discharge_type_desc VARCHAR(100) NOT NULL,
    is_active           BIT DEFAULT 1
);


-- ============================================================================
-- PHYSICIANS: Provider reference data
-- ============================================================================
CREATE TABLE source.physicians (
    physician_id        INT PRIMARY KEY,
    npi                 VARCHAR(10),            -- National Provider Identifier
    first_name          VARCHAR(50) NOT NULL,
    last_name           VARCHAR(50) NOT NULL,
    specialty           VARCHAR(100),
    department          VARCHAR(50),
    hire_date           DATE,
    is_active           BIT DEFAULT 1
);

CREATE INDEX IX_physicians_npi ON source.physicians(npi);
CREATE INDEX IX_physicians_specialty ON source.physicians(specialty);


-- ============================================================================
-- PATIENTS: Patient demographics (simplified)
-- ============================================================================
CREATE TABLE source.patients (
    patient_id          INT PRIMARY KEY,
    mrn                 VARCHAR(20),            -- Medical Record Number
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    date_of_birth       DATE,
    gender              CHAR(1),
    zip_code            VARCHAR(10),
    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IX_patients_mrn ON source.patients(mrn);
