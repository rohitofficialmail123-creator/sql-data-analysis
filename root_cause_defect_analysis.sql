/*
 * Root Cause & Defect Analysis — Final Review Queue
 *
 * Purpose: Analyze defect patterns in final review cases by joining
 *          decision data (filtered by root cause) with case metadata
 *          to identify reopen trends per defect type.
 *
 * Business Context:
 *   The disputes team requested an analysis of specific root cause categories
 *   in the final review queue to understand which defect types had the highest
 *   reopen rates. Results were used to prioritize process improvements and
 *   identify training gaps.
 *
 * Techniques:
 *   - CTEs for clean separation of filtering logic
 *   - Root cause filtering with multiple categories
 *   - Exclusion of incomplete records (status != 'Not Completed')
 *   - LEFT JOIN to preserve all decision records even without case matches
 *   - ORDER BY with NULLS LAST for clean sorting of reopen counts
 */

WITH decision_data AS (
    SELECT *
    FROM case_management.decisions
    WHERE date_closed BETWEEN '2024-12-01' AND '2024-12-31'
      AND root_cause IN (
          'Similar Product Found, Unrecoverable',
          'Shipment not delivered',
          'Seller mislabeling',
          'Short shipment - weight related',
          'Short shipment - non weight related'
      )
      AND processing_status IS NOT NULL
      AND processing_status != 'Not Completed'
),

case_data AS (
    SELECT *
    FROM case_management.cases
    WHERE queue_name IN (
        'hv-final-review@example.com',
        'hv-final-word@example.com'
    )
)

SELECT
    d.*,
    c.queue_name,
    c.reopen_count
FROM decision_data d
LEFT JOIN case_data c
    ON d.case_id = c.case_id
ORDER BY
    c.reopen_count DESC NULLS LAST;
