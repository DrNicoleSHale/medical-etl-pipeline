# Sample Data Dictionary

## Overview

This document describes the sample data included in the ETL pipeline for testing and demonstration purposes.

---

## Source Tables

### source.medical_events

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| event_id | INT | Unique identifier for each encounter | 1, 2, 3... |
| patient_id | INT | Foreign key to patients table | 1001, 1002... |
| physician_id | INT | Foreign key to physicians (nullable) | 101, 102, NULL |
| admit_date | DATE | Date patient was admitted | 2024-01-15 |
| discharge_date | DATE | Date patient was discharged (NULL if still admitted) | 2024-01-18, NULL |
| admission_type_id | INT | Foreign key to admission_types | 1 (ER), 3 (Elective) |
| discharge_type_id | INT | Foreign key to discharge_types | 1 (Home), 2 (SNF) |
| total_cost | DECIMAL | Total cost of encounter | 15000.00 |
| diagnosis_code | VARCHAR | ICD-10 diagnosis code | I21.0, J18.9 |
| department | VARCHAR | Department where care was provided | Heart Center |

### source.patients

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| patient_id | INT | Unique patient identifier | 1001 |
| mrn | VARCHAR | Medical Record Number | MRN001 |
| first_name | VARCHAR | Patient first name | John |
| last_name | VARCHAR | Patient last name | Smith |
| date_of_birth | DATE | Patient DOB | 1955-03-15 |
| gender | CHAR(1) | M/F | M, F |
| zip_code | VARCHAR | Patient zip code | 22101 |

### source.physicians

| Column | Type | Description | Sample Values |
|--------|------|-------------|---------------|
| physician_id | INT | Unique physician identifier | 101 |
| npi | VARCHAR | National Provider Identifier | 1234567890 |
| first_name | VARCHAR | Physician first name | Sarah |
| last_name | VARCHAR | Physician last name | Chen |
| specialty | VARCHAR | Medical specialty | Cardiology |
| department | VARCHAR | Department assignment | Heart Center |

---

## Test Scenarios Included

The sample data includes specific scenarios to test ETL logic:

| Scenario | Patient | Description |
|----------|---------|-------------|
| **Readmission** | 1001 | Discharged 1/18, readmitted 1/20 (2 days) |
| **Readmission** | 1007 | Discharged 3/10, readmitted 3/15 (5 days) |
| **NULL Physician** | 1009 | Event with no assigned physician |
| **Still Admitted** | 1010 | NULL discharge date and cost |
| **Pediatric** | 1008 | Child patient (DOB 2015) |
| **Elderly** | 1007 | 86-year-old patient |
| **To SNF** | 1003 | Discharged to Skilled Nursing |
| **To Hospice** | 1007 | Discharged to Hospice Care |

---

## Age Distribution in Sample Data

| Age Group | Count | Patients |
|-----------|-------|----------|
| 0-17 | 1 | 1008 |
| 18-44 | 2 | 1005, 1006 |
| 45-64 | 3 | 1002, 1004, 1009 |
| 65+ | 4 | 1001, 1003, 1007, 1010 |

---

## Specialty Distribution

| Specialty | Encounters | Physicians |
|-----------|------------|------------|
| Cardiology | 4 | 101, 109 |
| Internal Medicine | 3 | 104, 110 |
| Neurology | 2 | 103 |
| Emergency Medicine | 2 | 105 |
| Oncology | 2 | 106 |
| Orthopedics | 1 | 102 |
| Surgery | 1 | 107 |
| Pediatrics | 1 | 108 |
