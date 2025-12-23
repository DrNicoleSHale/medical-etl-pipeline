# Technical Decisions

## Overview

This document explains the key technical decisions made in designing this ETL pipeline, including trade-offs considered and rationale for each choice.

---

## 1. LEFT JOIN vs INNER JOIN

**Decision:** Use LEFT JOINs for all dimension lookups

**Rationale:**
- Healthcare data is messy - physician assignments may be missing, reference data may lag behind transactional data
- An INNER JOIN would silently drop encounters with missing physician IDs
- Losing encounters would skew cost analysis and patient outcome metrics
- Better to have a row with "Unassigned" physician than to lose the encounter entirely

**Trade-off:** Slightly more complex NULL handling in downstream queries
```sql
-- We do this:
LEFT JOIN source.physicians p ON e.physician_id = p.physician_id

-- Not this (would lose events):
INNER JOIN source.physicians p ON e.physician_id = p.physician_id
```

---

## 2. TRUNCATE/INSERT vs Incremental Load

**Decision:** Full reload (TRUNCATE and INSERT) for all target tables

**Rationale:**
- Simpler logic, easier to debug and maintain
- Source data volume is manageable (< 1M rows)
- Guarantees consistency - no risk of orphaned or duplicate records
- Appropriate for daily batch processing

**Trade-off:** Not suitable for very large datasets or real-time requirements

**When to consider incremental:**
- Source tables exceed 10M+ rows
- Need near-real-time updates
- Network/storage costs are a concern

---

## 3. Pre-calculated Fields

**Decision:** Calculate and store derived values (age_group, length_of_stay, physician_name)

**Rationale:**
- Query performance - avoid runtime calculations on every SELECT
- Consistency - everyone uses the same age group definitions
- Simplicity - business users can query without complex CASE statements

**Trade-off:** Data can become stale if source changes and ETL doesn't run

**Fields pre-calculated:**
| Field | Calculation |
|-------|-------------|
| length_of_stay | DATEDIFF(discharge, admit) |
| patient_age | DATEDIFF(admit, DOB) |
| age_group | CASE statement bucketing |
| physician_name | CONCAT(first, last) |

---

## 4. Readmission Window: 30 Days

**Decision:** Flag readmissions within 30 days as the primary metric

**Rationale:**
- Aligns with CMS Hospital Readmissions Reduction Program (HRRP)
- Industry standard metric that executives and clinicians understand
- Financial implications - CMS penalizes hospitals for excess readmissions

**Additional metric:** Also flag 7-day readmissions for internal quality review (more urgent concern)

---

## 5. Stored Procedures vs Views vs Scripts

**Decision:** Stored procedures for all transformations

**Rationale:**
- **Encapsulation** - logic is named, versioned, and reusable
- **Performance** - execution plans are cached
- **Security** - can grant EXEC without exposing underlying tables
- **Orchestration** - easy to call from scheduler or master procedure
- **Logging** - can add PRINT statements and error handling

**Alternative considered:** Views
- Views would work for simple transformations
- But can't do TRUNCATE/INSERT pattern
- Harder to add logging and error handling

---

## 6. Date Truncation for Periods

**Decision:** Use DATEFROMPARTS(YEAR, MONTH, 1) for monthly grouping

**Rationale:**
- Creates consistent first-of-month dates for grouping
- Easier to join to calendar/fiscal period tables
- More readable than DATETRUNC in some SQL dialects
```sql
DATEFROMPARTS(YEAR(admit_date), MONTH(admit_date), 1) AS summary_period
```

---

## 7. COALESCE for NULL Handling

**Decision:** Explicit COALESCE with meaningful defaults throughout

**Rationale:**
- NULLs propagate silently and cause unexpected results
- Business users expect to see "Unknown" not blank cells
- Aggregations handle "Unknown" better than NULL

**Standard defaults:**
| Field Type | Default |
|------------|---------|
| Names | 'Unassigned' or 'Unknown' |
| Counts | 0 |
| Flags | 0 (false) |
| Dates | Leave NULL (don't fabricate dates) |

---

## 8. Schema Separation

**Decision:** Separate schemas for source, analytics, and staging

**Rationale:**
- Clear ownership and purpose for each table
- Easier to manage permissions (analysts can read analytics.*, not source.*)
- Prevents accidental modification of source data
- Self-documenting architecture

**Schema structure:**
```
source.*      -- Raw data from upstream systems (read-only)
analytics.*   -- Transformed, business-ready tables
staging.*     -- Temporary tables for complex transformations (if needed)
```

---

## 9. Identity Columns for Surrogate Keys

**Decision:** Use IDENTITY columns for analytics table surrogate keys

**Rationale:**
- Simple, auto-incrementing, guaranteed unique
- No business meaning - won't need to change if business rules change
- Efficient for joins (integer comparison)

**Exception:** Tables with natural keys from source (like event_id) retain those keys

---

## 10. Error Handling Strategy

**Decision:** Fail-fast with informative messages (for this demo)

**Production enhancement:** Would add TRY/CATCH blocks with:
- Error logging to audit table
- Transaction rollback on failure
- Alert/notification triggers
- Retry logic for transient failures
```sql
-- Production pattern (not implemented in demo):
BEGIN TRY
    BEGIN TRANSACTION
    -- ETL logic here
    COMMIT
END TRY
BEGIN CATCH
    ROLLBACK
    -- Log error details
    -- Send alert
    THROW
END CATCH
```
