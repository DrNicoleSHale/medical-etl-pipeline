-- ============================================================================
-- SAMPLE DATA FOR TESTING
-- ============================================================================
-- PURPOSE: Populate source tables with realistic test data.
--          This allows the ETL procedures to be tested and demonstrated.
--
-- NOTE: In production, this data would come from upstream system feeds.
-- ============================================================================

-- ============================================================================
-- REFERENCE DATA: Admission Types
-- ============================================================================
INSERT INTO source.admission_types (admission_type_id, admission_type_code, admission_type_desc, is_emergency) VALUES
(1, 'ER', 'Emergency Room', 1),
(2, 'URG', 'Urgent Care', 1),
(3, 'ELE', 'Elective/Scheduled', 0),
(4, 'OBS', 'Observation', 0),
(5, 'TRN', 'Transfer from Another Facility', 0),
(6, 'NB', 'Newborn', 0);


-- ============================================================================
-- REFERENCE DATA: Discharge Types
-- ============================================================================
INSERT INTO source.discharge_types (discharge_type_id, discharge_type_code, discharge_type_desc) VALUES
(1, 'HOME', 'Discharged to Home'),
(2, 'SNF', 'Skilled Nursing Facility'),
(3, 'REHAB', 'Rehabilitation Facility'),
(4, 'AMA', 'Left Against Medical Advice'),
(5, 'TRANS', 'Transferred to Another Hospital'),
(6, 'EXP', 'Expired'),
(7, 'HOS', 'Hospice Care');


-- ============================================================================
-- REFERENCE DATA: Physicians
-- ============================================================================
INSERT INTO source.physicians (physician_id, npi, first_name, last_name, specialty, department) VALUES
(101, '1234567890', 'Sarah', 'Chen', 'Cardiology', 'Heart Center'),
(102, '2345678901', 'Michael', 'Johnson', 'Orthopedics', 'Musculoskeletal'),
(103, '3456789012', 'Emily', 'Williams', 'Neurology', 'Neuroscience'),
(104, '4567890123', 'David', 'Brown', 'Internal Medicine', 'General Medicine'),
(105, '5678901234', 'Jessica', 'Davis', 'Emergency Medicine', 'Emergency Dept'),
(106, '6789012345', 'Robert', 'Miller', 'Oncology', 'Cancer Center'),
(107, '7890123456', 'Amanda', 'Wilson', 'Surgery', 'Surgical Services'),
(108, '8901234567', 'James', 'Taylor', 'Pediatrics', 'Childrens Center'),
(109, '9012345678', 'Lisa', 'Anderson', 'Cardiology', 'Heart Center'),
(110, '0123456789', 'Kevin', 'Thomas', 'Internal Medicine', 'General Medicine');


-- ============================================================================
-- REFERENCE DATA: Patients
-- ============================================================================
INSERT INTO source.patients (patient_id, mrn, first_name, last_name, date_of_birth, gender, zip_code) VALUES
(1001, 'MRN001', 'John', 'Smith', '1955-03-15', 'M', '22101'),
(1002, 'MRN002', 'Mary', 'Johnson', '1968-07-22', 'F', '22102'),
(1003, 'MRN003', 'Robert', 'Williams', '1945-11-30', 'M', '22103'),
(1004, 'MRN004', 'Patricia', 'Brown', '1978-04-18', 'F', '22104'),
(1005, 'MRN005', 'Michael', 'Jones', '1982-09-05', 'M', '22105'),
(1006, 'MRN006', 'Jennifer', 'Garcia', '1990-01-12', 'F', '22106'),
(1007, 'MRN007', 'William', 'Miller', '1938-06-28', 'M', '22107'),
(1008, 'MRN008', 'Elizabeth', 'Davis', '2015-08-20', 'F', '22108'),
(1009, 'MRN009', 'David', 'Rodriguez', '1972-12-03', 'M', '22109'),
(1010, 'MRN010', 'Susan', 'Martinez', '1960-05-25', 'F', '22110');


-- ============================================================================
-- TRANSACTIONAL DATA: Medical Events
-- Variety of scenarios for testing ETL logic
-- ============================================================================
INSERT INTO source.medical_events (event_id, patient_id, physician_id, admit_date, discharge_date, admission_type_id, discharge_type_id, total_cost, diagnosis_code, department) VALUES
-- Patient 1001: Multiple visits, including a readmission scenario
(1, 1001, 101, '2024-01-15', '2024-01-18', 1, 1, 15000.00, 'I21.0', 'Heart Center'),
(2, 1001, 101, '2024-01-20', '2024-01-22', 1, 1, 8500.00, 'I21.0', 'Heart Center'),      -- Readmit within 3 days!
(3, 1001, 104, '2024-06-10', '2024-06-12', 3, 1, 4200.00, 'J18.9', 'General Medicine'),

-- Patient 1002: Elective surgery
(4, 1002, 107, '2024-02-20', '2024-02-24', 3, 1, 32000.00, 'K80.2', 'Surgical Services'),

-- Patient 1003: Elderly patient, multiple specialists
(5, 1003, 103, '2024-03-05', '2024-03-15', 1, 2, 45000.00, 'I63.9', 'Neuroscience'),     -- Stroke, to SNF
(6, 1003, 101, '2024-07-20', '2024-07-25', 3, 1, 18000.00, 'I25.1', 'Heart Center'),

-- Patient 1004: Routine visits
(7, 1004, 104, '2024-01-30', '2024-01-31', 4, 1, 2100.00, 'R10.9', 'General Medicine'),
(8, 1004, 102, '2024-04-15', '2024-04-18', 3, 1, 28000.00, 'M17.1', 'Musculoskeletal'),  -- Knee replacement

-- Patient 1005: ER visits
(9, 1005, 105, '2024-02-14', '2024-02-14', 1, 1, 3500.00, 'S52.5', 'Emergency Dept'),
(10, 1005, 105, '2024-08-22', '2024-08-23', 1, 1, 4800.00, 'K35.8', 'Emergency Dept'),

-- Patient 1006: Young healthy patient, single visit
(11, 1006, 104, '2024-05-10', '2024-05-11', 4, 1, 1800.00, 'J06.9', 'General Medicine'),

-- Patient 1007: Elderly, oncology + readmission
(12, 1007, 106, '2024-03-01', '2024-03-10', 3, 1, 52000.00, 'C34.9', 'Cancer Center'),
(13, 1007, 106, '2024-03-15', '2024-03-20', 1, 7, 28000.00, 'C34.9', 'Cancer Center'),   -- Readmit, to hospice

-- Patient 1008: Pediatric
(14, 1008, 108, '2024-04-05', '2024-04-06', 1, 1, 2800.00, 'J21.0', 'Childrens Center'),

-- Patient 1009: Missing physician (test NULL handling)
(15, 1009, NULL, '2024-06-15', '2024-06-17', 1, 1, 6500.00, 'N39.0', 'General Medicine'),

-- Patient 1010: Still admitted (NULL discharge)
(16, 1010, 103, '2024-09-01', NULL, 1, NULL, NULL, 'G45.9', 'Neuroscience');


-- ============================================================================
-- VERIFY DATA LOAD
-- ============================================================================
SELECT 'admission_types' AS table_name, COUNT(*) AS row_count FROM source.admission_types
UNION ALL SELECT 'discharge_types', COUNT(*) FROM source.discharge_types
UNION ALL SELECT 'physicians', COUNT(*) FROM source.physicians
UNION ALL SELECT 'patients', COUNT(*) FROM source.patients
UNION ALL SELECT 'medical_events', COUNT(*) FROM source.medical_events;
