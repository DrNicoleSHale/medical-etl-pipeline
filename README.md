# Medical Data ETL Pipeline

## Overview
A comprehensive SQL-based ETL system that transforms raw medical encounter data into analytics-ready tables for healthcare reporting. This project demonstrates advanced SQL techniques including complex joins, window functions, data pivoting, and business logic implementation.

## Business Problem
Healthcare organizations collect transactional data from multiple systems (admissions, discharges, physicians, costs) but need consolidated, clean data for:
- Patient analytics and outcomes tracking
- Readmission analysis for quality metrics (CMS penalties)
- Cost analysis and resource planning
- Physician performance reporting

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           SOURCE LAYER                                   │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐   │
│  │ medical_     │ │ admission_   │ │ discharge_   │ │ physicians   │   │
│  │ events       │ │ types        │ │ types        │ │              │   │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         TRANSFORM LAYER                                  │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  Stored Procedures:                                             │    │
│  │  • usp_load_medical_encounters (base fact table)               │    │
│  │  • usp_load_cost_summary (aggregations)                        │    │
│  │  • usp_load_first_visits (window functions)                    │    │
│  │  • usp_load_readmissions (self-join logic)                     │    │
│  │  • usp_load_specialty_counts (pivot transformation)            │    │
│  └────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          TARGET LAYER                                    │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐   │
│  │ medical_     │ │ cost_        │ │ first_       │ │ readmission_ │   │
│  │ encounters   │ │ summary      │ │ visits       │ │ analysis     │   │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

## Technical Implementation

### 1. Base Fact Table Load (`usp_load_medical_encounters`)
Combines data from 5 source tables with complex business logic:

```sql
-- Key techniques demonstrated:
-- • Multi-table LEFT JOINs (preserve events even with missing reference data)
-- • CASE statements for age group classification
-- • Date formatting and calculations
-- • NULL handling with COALESCE
-- • Data type conversions
```

**Design Decision:** Used LEFT JOINs instead of INNER JOINs to preserve all medical events. In healthcare analytics, losing events due to missing physician info would skew cost analysis and patient outcomes.

### 2. Cost Summary Aggregation (`usp_load_cost_summary`)
Aggregates costs by admission type and time period:

```sql
-- Key techniques demonstrated:
-- • GROUP BY with multiple dimensions
-- • Aggregate functions (SUM, AVG, COUNT)
-- • Date truncation for period analysis
```

### 3. Patient First Visit Analysis (`usp_load_first_visits`)
Identifies each patient's earliest encounter using window functions:

```sql
-- Key techniques demonstrated:
-- • ROW_NUMBER() window function
-- • PARTITION BY for patient-level ranking
-- • CTE for clean query structure
```

### 4. Readmission Detection (`usp_load_readmissions`)
Identifies patients readmitted within 3 days (CMS quality metric):

```sql
-- Key techniques demonstrated:
-- • Self-join on patient ID
-- • DATEDIFF for interval calculation
-- • Business rule implementation
```

**Design Decision:** 3-day window aligns with CMS Hospital Readmissions Reduction Program (HRRP) metrics.

### 5. Specialty Analytics (`usp_load_specialty_counts`)
Pivots patient counts by physician specialty:

```sql
-- Key techniques demonstrated:
-- • PIVOT operator for crosstab transformation
-- • Dynamic column generation
-- • Aggregation within pivot
```

## Data Quality Considerations

| Issue | Handling Approach |
|-------|------------------|
| Missing physician IDs | LEFT JOIN preserves events; NULL specialty reported |
| Invalid dates | COALESCE to default; flagged in quality column |
| Duplicate events | ROW_NUMBER() deduplication in staging |
| Inconsistent codes | CASE statement normalization |

## How to Run

1. **Create schemas:**
```sql
CREATE SCHEMA source;
CREATE SCHEMA analytics;
CREATE SCHEMA reporting;
```

2. **Run setup scripts in order:**
```bash
sql/01_create_source_tables.sql
sql/02_create_target_tables.sql
sql/03_load_sample_data.sql
sql/04_create_procedures.sql
```

3. **Execute ETL pipeline:**
```sql
EXEC usp_load_medical_encounters;
EXEC usp_load_cost_summary;
EXEC usp_load_first_visits;
EXEC usp_load_readmissions;
EXEC usp_load_specialty_counts;
```

## Files
```
01-medical-etl-pipeline/
├── README.md
├── sql/
│   ├── 01_create_source_tables.sql
│   ├── 02_create_target_tables.sql
│   ├── 03_load_sample_data.sql
│   └── 04_create_procedures.sql
├── data/
│   └── sample_data_dictionary.md
└── docs/
    └── technical_decisions.md
```

## Key Learnings
- Always preserve data with LEFT JOINs when data completeness is uncertain
- TRUNCATE/INSERT pattern is faster than DELETE for full reloads
- Window functions enable complex analytics without self-joins
- Business context (like CMS metrics) should drive technical decisions
