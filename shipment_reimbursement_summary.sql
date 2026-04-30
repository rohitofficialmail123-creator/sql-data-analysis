/*
 * Shipment-Level Reimbursement Summary — Multi-CTE Pipeline
 *
 * Purpose: Aggregate case-level data up to the shipment level, computing
 *          total reimbursements, case counts, and consolidating queue names
 *          and case IDs per shipment for a two-month analysis window.
 *
 * Business Context:
 *   Leadership requested a shipment-level view of reimbursement activity
 *   across final review queues for Apr-May 2025. Individual cases needed
 *   to be rolled up to the shipment level to identify high-value shipments
 *   with multiple cases and compare missing value vs actual reimbursement.
 *
 * Techniques:
 *   - 4 CTEs for modular, readable aggregation pipeline
 *   - COUNT(DISTINCT) for accurate case counting per shipment
 *   - LISTAGG with WITHIN GROUP for string aggregation (comma-separated
 *     case IDs and queue names per shipment)
 *   - SUM aggregation for total reimbursement per shipment
 *   - COALESCE to handle missing values across joined tables
 *   - Multiple LEFT JOINs to combine all aggregation dimensions
 */

WITH base_data AS (
    -- Step 1: Get distinct resolved cases from final review queues
    SELECT DISTINCT
        c.case_id,
        c.shipment_id,
        c.creation_date,
        c.resolve_date,
        c.queue_name,
        c.reimbursed_amount,
        c.missing_value
    FROM case_management.shipment_case_data c
    WHERE c.resolve_date BETWEEN '2025-04-01' AND '2025-05-31'
      AND c.queue_name IN (
          'hv-final-review@example.com',
          'ineligible-final-review@example.com',
          'hv-final-word@example.com',
          'ar-final-word@example.com'
      )
),

case_counts AS (
    -- Step 2: Count distinct cases per shipment
    SELECT
        shipment_id,
        COUNT(DISTINCT case_id) AS number_of_cases
    FROM base_data
    GROUP BY shipment_id
),

queue_agg AS (
    -- Step 3: Concatenate distinct queue names per shipment
    SELECT
        shipment_id,
        LISTAGG(DISTINCT queue_name, ', ')
            WITHIN GROUP (ORDER BY queue_name) AS queue_names
    FROM base_data
    GROUP BY shipment_id
),

reimbursement_sum AS (
    -- Step 4: Total reimbursement per shipment
    SELECT
        shipment_id,
        SUM(reimbursed_amount) AS total_shipment_reimbursement
    FROM base_data
    GROUP BY shipment_id
)

-- Step 5: Final assembly — join all aggregations back to base data
SELECT
    b.shipment_id,
    MIN(b.creation_date)                                    AS first_creation_date,
    MAX(b.resolve_date)                                     AS last_resolve_date,
    MAX(COALESCE(d.total_missing_value, b.missing_value))   AS missing_value,
    rs.total_shipment_reimbursement,
    cc.number_of_cases,
    qa.queue_names,
    LISTAGG(b.case_id, ', ')
        WITHIN GROUP (ORDER BY b.case_id)                   AS case_ids
FROM base_data b
LEFT JOIN case_management.decisions d
    ON b.case_id = d.case_id
LEFT JOIN case_counts cc
    ON b.shipment_id = cc.shipment_id
LEFT JOIN queue_agg qa
    ON b.shipment_id = qa.shipment_id
LEFT JOIN reimbursement_sum rs
    ON b.shipment_id = rs.shipment_id
GROUP BY
    b.shipment_id,
    cc.number_of_cases,
    qa.queue_names,
    rs.total_shipment_reimbursement;
