/*
 * Shipment-Level Reimbursement Analysis
 *
 * Purpose: Identify shipments with missing units and calculate reimbursement
 *          amounts by joining shipment data with the reimbursement decisions table.
 *
 * Business Context:
 *   Operations leadership needed visibility into which inbound shipments had
 *   missing units and how much was approved for reimbursement. This query was
 *   used to audit a batch of ~500 flagged shipments for a monthly review.
 *
 * Techniques:
 *   - LEFT JOIN with aggregated subquery to get max reimbursement per case
 *   - CASE expression inside MAX() to handle conditional aggregation
 *   - COALESCE for null-safe reimbursement defaulting
 *   - Computed column (expected - missing = received)
 */

SELECT DISTINCT
    s.shipment_id,
    s.num_skus_in_shipment          AS expected_skus,
    s.num_skus_with_missing_units   AS skus_with_missing,
    s.total_expected_units,
    s.total_missing_units,
    s.total_expected_units - s.total_missing_units AS total_received_units,
    COALESCE(r.units_to_reimburse, 0) AS total_units_to_reimburse
FROM
    warehouse_ops.shipment_case_data s
    LEFT JOIN (
        -- Get the max positive reimbursement per case to avoid duplicates
        SELECT
            case_id,
            MAX(
                CASE
                    WHEN units_to_reimburse > 0 THEN units_to_reimburse
                    ELSE 0
                END
            ) AS units_to_reimburse
        FROM
            warehouse_ops.reimbursement_decisions
        GROUP BY
            case_id
    ) r ON s.case_id = r.case_id
WHERE
    -- Filter to a specific batch of flagged shipments
    s.shipment_id IN (
        'SHIP-001', 'SHIP-002', 'SHIP-003', 'SHIP-004', 'SHIP-005',
        'SHIP-006', 'SHIP-007', 'SHIP-008', 'SHIP-009', 'SHIP-010'
        -- ... ~500 shipment IDs were provided by the operations team
        -- for a monthly reimbursement audit
    )
GROUP BY
    s.shipment_id,
    s.num_skus_in_shipment,
    s.num_skus_with_missing_units,
    s.total_expected_units,
    s.total_missing_units,
    r.units_to_reimburse;
